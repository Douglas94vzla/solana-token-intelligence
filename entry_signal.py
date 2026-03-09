import asyncio
import aiohttp
import psycopg2
import psycopg2.pool
import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/entry_signal.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_snapshots (
                id SERIAL PRIMARY KEY,
                mint TEXT REFERENCES discovered_tokens(mint),
                price_usd NUMERIC(20,10),
                market_cap NUMERIC(20,2),
                volume NUMERIC(20,2),
                buys INTEGER,
                sells INTEGER,
                snapshot_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_snapshots_mint ON price_snapshots(mint);
            CREATE INDEX IF NOT EXISTS idx_snapshots_time ON price_snapshots(snapshot_at DESC);
            
            ALTER TABLE discovered_tokens
            ADD COLUMN IF NOT EXISTS entry_signal TEXT DEFAULT 'WAIT',
            ADD COLUMN IF NOT EXISTS entry_price NUMERIC(20,10),
            ADD COLUMN IF NOT EXISTS entry_at TIMESTAMP,
            ADD COLUMN IF NOT EXISTS price_change_15m NUMERIC(10,2),
            ADD COLUMN IF NOT EXISTS price_change_1h NUMERIC(10,2);
        """)
        conn.commit()
        cur.close()
        log.info("✅ Tablas de señales creadas")
    finally:
        pool.putconn(conn)

async def fetch_current_data(session, mint):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            pairs = data.get("pairs")
            if not pairs:
                return None
            pair = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
            return {
                "price_usd": pair.get("priceUsd"),
                "market_cap": pair.get("marketCap"),
                "volume": pair.get("volume", {}).get("h24", 0),
                "buys_1h": pair.get("txns", {}).get("h1", {}).get("buys", 0),
                "sells_1h": pair.get("txns", {}).get("h1", {}).get("sells", 0),
                "buys_5m": pair.get("txns", {}).get("m5", {}).get("buys", 0),
                "sells_5m": pair.get("txns", {}).get("m5", {}).get("sells", 0),
                "price_change_5m": pair.get("priceChange", {}).get("m5", 0),
                "price_change_1h": pair.get("priceChange", {}).get("h1", 0),
            }
    except Exception:
        return None

def save_snapshot(mint, data):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO price_snapshots (mint, price_usd, market_cap, volume, buys, sells)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            mint, data["price_usd"], data["market_cap"],
            data["volume"], data["buys_5m"], data["sells_5m"]
        ))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_price_history(mint):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT price_usd, market_cap, volume, buys, sells, snapshot_at
            FROM price_snapshots
            WHERE mint = %s
            ORDER BY snapshot_at DESC
            LIMIT 10
        """, (mint,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def compute_entry_signal(current, history):
    """
    Señal de entrada basada en momentum de precio.
    
    Condiciones para ENTER:
    1. Precio subiendo en últimos 2 snapshots consecutivos
    2. Más compras que ventas en 5m
    3. Volumen acelerando
    4. Price change 5m positivo
    
    Condiciones para EXIT:
    1. Precio bajando 2 snapshots consecutivos
    2. Ventas superan compras
    """
    if not history or len(history) < 2:
        return 'WAIT', 0

    prices = [float(r[0]) for r in history if r[0]]
    buys = [r[3] for r in history if r[3] is not None]
    sells = [r[4] for r in history if r[4] is not None]

    if len(prices) < 2:
        return 'WAIT', 0

    # Momentum de precio
    price_up = prices[0] > prices[1]  # último > anterior
    price_acceleration = (prices[0] - prices[1]) / prices[1] * 100 if prices[1] > 0 else 0

    # Presión compradora
    buy_pressure = buys[0] > sells[0] if buys and sells else False
    buy_ratio = buys[0] / (buys[0] + sells[0]) if buys and (buys[0] + sells[0]) > 0 else 0

    # Momentum score
    momentum = 0
    if price_up:
        momentum += 30
    if price_acceleration > 5:
        momentum += 20
    elif price_acceleration > 2:
        momentum += 10
    if buy_pressure:
        momentum += 25
    if buy_ratio > 0.7:
        momentum += 25
    elif buy_ratio > 0.5:
        momentum += 10

    # Señal final
    if momentum >= 70:
        return 'ENTER', momentum
    elif momentum >= 40:
        return 'WATCH', momentum
    elif not price_up and not buy_pressure:
        return 'EXIT', momentum
    else:
        return 'WAIT', momentum

def update_entry_signal(mint, signal, price):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        if signal == 'ENTER':
            cur.execute("""
                UPDATE discovered_tokens
                SET entry_signal = %s, entry_price = %s, entry_at = NOW()
                WHERE mint = %s AND entry_signal != 'ENTER'
            """, (signal, price, mint))
        else:
            cur.execute("""
                UPDATE discovered_tokens SET entry_signal = %s WHERE mint = %s
            """, (signal, mint))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_watchlist():
    """Tokens con buen score que monitoreamos activamente"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, name, symbol, survival_score, price_usd, market_cap
            FROM discovered_tokens
            WHERE signal IN ('STRONG_BUY', 'BUY')
            AND created_at > NOW() - INTERVAL '6 hours'
            AND price_usd IS NOT NULL
            ORDER BY survival_score DESC
            LIMIT 50
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

async def monitor_cycle():
    watchlist = get_watchlist()
    if not watchlist:
        log.info("Sin tokens en watchlist")
        return

    log.info(f"📡 Monitoreando {len(watchlist)} tokens...")

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(5)

        async def process_token(token):
            mint, name, symbol, score, orig_price, orig_mcap = token
            async with semaphore:
                data = await fetch_current_data(session, mint)
                if not data or not data["price_usd"]:
                    return

                save_snapshot(mint, data)
                history = get_price_history(mint)
                signal, momentum = compute_entry_signal(data, history)
                update_entry_signal(mint, signal, data["price_usd"])

                curr_price = float(data["price_usd"])
                orig = float(orig_price) if orig_price else curr_price
                change_pct = ((curr_price - orig) / orig * 100) if orig > 0 else 0

                if signal == 'ENTER':
                    log.info(
                        f"🚨 ENTER SIGNAL | {name or mint[:8]} ({symbol}) | "
                        f"Score: {score} | Momentum: {momentum} | "
                        f"Price: ${curr_price:.8f} | Change: {change_pct:+.1f}% | "
                        f"Buys5m: {data['buys_5m']} | Sells5m: {data['sells_5m']}"
                    )
                elif signal == 'EXIT':
                    log.info(
                        f"🔴 EXIT SIGNAL | {name or mint[:8]} | "
                        f"Change: {change_pct:+.1f}%"
                    )

        await asyncio.gather(*[process_token(t) for t in watchlist])

async def main():
    setup_db()
    log.info("🎯 Entry Signal Engine arrancando...")

    while True:
        try:
            await monitor_cycle()
            log.info("⏳ Esperando 60 segundos...")
            await asyncio.sleep(60)
        except Exception as e:
            log.error(f"Error en ciclo: {e}")
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
