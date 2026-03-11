import asyncio
import aiohttp
import json
import websockets
import os
import base64
import base58
import psycopg2
import psycopg2.pool
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/harvester.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

WSS_URL = os.getenv("WSS_URL")
PUMP_FUN_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

seen_mints = set()

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

async def fetch_initial_price(mint):
    """Consulta DexScreener 10 segundos después del lanzamiento"""
    await asyncio.sleep(10)
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                pairs = data.get("pairs")
                if not pairs:
                    return None
                pair = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
                return {
                    "price_usd":     pair.get("priceUsd"),
                    "market_cap":    pair.get("marketCap"),
                    "volume_24h":    pair.get("volume", {}).get("h24"),
                    "buys_5m":       pair.get("txns", {}).get("m5", {}).get("buys"),
                    "sells_5m":      pair.get("txns", {}).get("m5", {}).get("sells"),
                    "pair_address":  pair.get("pairAddress"),
                    "liquidity_usd": pair.get("liquidity", {}).get("usd"),
                    "fdv":           pair.get("fdv"),
                }
    except Exception as e:
        log.warning(f"Price fetch error para {mint}: {e}")
        return None

async def save_token(mint):
    if mint in seen_mints:
        return
    seen_mints.add(mint)

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO discovered_tokens (mint, status) VALUES (%s, %s) ON CONFLICT (mint) DO NOTHING RETURNING id",
            (mint, 'new')
        )
        result = cur.fetchone()
        conn.commit()
        cur.close()

        if result:
            log.info(f"🚀 NUEVO TOKEN: {mint}")
            # Fetch precio inicial en background
            asyncio.create_task(save_initial_price(mint))
        
    except Exception as e:
        log.error(f"❌ Error DB: {e}")
    finally:
        pool.putconn(conn)

async def save_initial_price(mint):
    price_data = await fetch_initial_price(mint)
    if not price_data:
        return
    
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE discovered_tokens SET
                price_usd     = %s,
                market_cap    = %s,
                volume_24h    = %s,
                buys_5m       = %s,
                sells_5m      = %s,
                pair_address  = %s,
                liquidity_usd = %s,
                fdv           = %s,
                price_updated_at = NOW()
            WHERE mint = %s
        """, (
            price_data["price_usd"],
            price_data["market_cap"],
            price_data["volume_24h"],
            price_data["buys_5m"],
            price_data["sells_5m"],
            price_data["pair_address"],
            price_data["liquidity_usd"],
            price_data["fdv"],
            mint
        ))
        conn.commit()
        cur.close()
        log.info(f"💰 Precio inicial guardado: {mint} @ ${price_data['price_usd']}")
    except Exception as e:
        log.error(f"❌ Error guardando precio: {e}")
    finally:
        pool.putconn(conn)

async def harvester():
    log.info("📡 Harvester iniciando...")
    while True:
        try:
            async with websockets.connect(WSS_URL, ping_interval=20, ping_timeout=30) as ws:
                payload = {
                    "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                    "params": [{"mentions": [PUMP_FUN_ID]}, {"commitment": "processed"}]
                }
                await ws.send(json.dumps(payload))
                log.info("✅ WebSocket conectado")

                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)

                    if "params" in data:
                        logs = data["params"]["result"]["value"]["logs"]
                        if any("Instruction: Create" in l for l in logs):
                            for log_line in logs:
                                if "Program data:" in log_line:
                                    try:
                                        raw_data = base64.b64decode(log_line.split("Program data: ")[1])
                                        mint_bytes = raw_data[8:40]
                                        mint = base58.b58encode(mint_bytes).decode('utf-8')
                                        if mint.endswith("pump"):
                                            await save_token(mint)
                                    except:
                                        continue
                                elif "mint:" in log_line.lower():
                                    mint = log_line.split("mint:")[1].split(",")[0].strip()
                                    await save_token(mint)

        except Exception as e:
            log.warning(f"⚠️ Conexión perdida: {e}. Reconectando en 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(harvester())
