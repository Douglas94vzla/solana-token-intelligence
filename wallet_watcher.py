import asyncio
import websockets
import requests
import psycopg2
import psycopg2.pool
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/wallet_watcher.log'),
    ]
)
log = logging.getLogger(__name__)

WSS_URL = os.getenv("WSS_URL")
RPC_URL = os.getenv("RPC_URL")

# ── FILTROS ───────────────────────────────────────────
MAX_MCAP_ALERT = 100_000   # Solo alertar si MCap < $100K
MIN_MCAP_ALERT = 1_000     # Solo alertar si MCap > $1K
COOLDOWN_SECONDS = 300     # No repetir alerta del mismo mint en 5 min

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

# Cache para evitar alertas duplicadas
alerted_mints = {}

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallet_alerts (
                id SERIAL PRIMARY KEY,
                wallet TEXT NOT NULL,
                mint TEXT,
                action TEXT,
                mcap NUMERIC,
                signature TEXT,
                alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            ALTER TABLE wallet_alerts ADD COLUMN IF NOT EXISTS mcap NUMERIC;
        """)
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_smart_wallets():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT wallet, wins, win_rate
            FROM smart_wallets
            WHERE is_smart = TRUE
            AND wins >= 3
            ORDER BY wins DESC
            LIMIT 15
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def get_transaction_mint(signature):
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTransaction",
        "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    }
    try:
        resp = requests.post(RPC_URL, json=payload, timeout=10).json()
        result = resp.get("result")
        if not result:
            return None
        instructions = result.get("transaction", {}).get("message", {}).get("instructions", [])
        for ix in instructions:
            mint = ix.get("parsed", {}).get("info", {}).get("mint", "")
            if mint.endswith("pump"):
                return mint
        for group in result.get("meta", {}).get("innerInstructions", []):
            for ix in group.get("instructions", []):
                mint = ix.get("parsed", {}).get("info", {}).get("mint", "")
                if mint.endswith("pump"):
                    return mint
        return None
    except Exception:
        return None

def get_token_info(mint):
    try:
        resp = requests.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{mint}",
            timeout=8
        ).json()
        pairs = resp.get("pairs")
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
        return {
            "name": pair.get("baseToken", {}).get("name", "Unknown"),
            "symbol": pair.get("baseToken", {}).get("symbol", "???"),
            "price": pair.get("priceUsd", 0),
            "mcap": pair.get("marketCap", 0),
            "volume": pair.get("volume", {}).get("h24", 0),
            "buys_5m": pair.get("txns", {}).get("m5", {}).get("buys", 0),
            "sells_5m": pair.get("txns", {}).get("m5", {}).get("sells", 0),
        }
    except Exception:
        return None

def should_alert(mint, mcap):
    """Filtro: MCap dentro de rango y sin alerta reciente"""
    if not mcap or mcap > MAX_MCAP_ALERT or mcap < MIN_MCAP_ALERT:
        return False
    last = alerted_mints.get(mint, 0)
    if (datetime.now().timestamp() - last) < COOLDOWN_SECONDS:
        return False
    return True

def save_alert(wallet, mint, mcap, signature):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO wallet_alerts (wallet, mint, action, mcap, signature)
            VALUES (%s, %s, 'BUY', %s, %s)
        """, (wallet, mint, mcap, signature))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def print_alert(wallet, mint, token_info, wins):
    name   = token_info.get("name", "Unknown")
    symbol = token_info.get("symbol", "???")
    mcap   = token_info.get("mcap", 0)
    price  = token_info.get("price", 0)
    buys   = token_info.get("buys_5m", 0)
    sells  = token_info.get("sells_5m", 0)

    print("\n" + "🚨" * 25)
    print(f"  🧠 SMART WALLET COMPRÓ — {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Wallet:  {wallet[:20]}... ({wins} wins)")
    print(f"  Token:   {name} ({symbol})")
    print(f"  Mint:    {mint}")
    print(f"  MCap:    ${mcap:,.0f}")
    print(f"  Precio:  ${price}")
    print(f"  Buys5m:  {buys} | Sells5m: {sells}")
    print(f"  ⚡ CONSIDERA ENTRAR AHORA")
    print("🚨" * 25 + "\n")

async def watch_wallet(wallet, wins, win_rate):
    log.info(f"👁️  {wallet[:20]}... | Wins: {wins}")

    subscribe_msg = {
        "jsonrpc": "2.0", "id": 1,
        "method": "logsSubscribe",
        "params": [{"mentions": [wallet]}, {"commitment": "confirmed"}]
    }

    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=30, ping_timeout=10) as ws:
                await ws.send(json.dumps(subscribe_msg))
                await ws.recv()  # confirmación de suscripción

                async for message in ws:
                    data = json.loads(message)
                    value = data.get("params", {}).get("result", {}).get("value", {})
                    logs  = value.get("logs", [])
                    sig   = value.get("signature", "")

                    is_buy = any("buy" in l.lower() or "swap" in l.lower() for l in logs)
                    if not is_buy or not sig:
                        continue

                    await asyncio.sleep(2)
                    mint = get_transaction_mint(sig)
                    if not mint:
                        continue

                    token_info = get_token_info(mint)
                    if not token_info:
                        continue

                    mcap = token_info.get("mcap", 0)
                    if not should_alert(mint, mcap):
                        continue

                    alerted_mints[mint] = datetime.now().timestamp()
                    save_alert(wallet, mint, mcap, sig)
                    print_alert(wallet, mint, token_info, wins)

        except Exception as e:
            log.warning(f"Reconectando {wallet[:16]}: {e}")
            await asyncio.sleep(5)

async def main():
    setup_db()
    wallets = get_smart_wallets()
    if not wallets:
        log.error("❌ Sin smart wallets — corre smart_money.py primero")
        return

    log.info(f"🎯 Monitoreando {len(wallets)} smart wallets (MCap filter: <${MAX_MCAP_ALERT:,})")
    tasks = [watch_wallet(w, wins, wr) for w, wins, wr in wallets]
    await asyncio.sleep(1)
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
