import asyncio
import aiohttp
import psycopg2
import psycopg2.pool
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/price_fetcher.log'),
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

# Primero añadimos las columnas de precio a la DB
def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            ALTER TABLE discovered_tokens 
            ADD COLUMN IF NOT EXISTS price_usd NUMERIC(20,10),
            ADD COLUMN IF NOT EXISTS market_cap NUMERIC(20,2),
            ADD COLUMN IF NOT EXISTS volume_24h NUMERIC(20,2),
            ADD COLUMN IF NOT EXISTS volume_1h NUMERIC(20,2),
            ADD COLUMN IF NOT EXISTS volume_5m NUMERIC(20,2),
            ADD COLUMN IF NOT EXISTS buys_24h INTEGER,
            ADD COLUMN IF NOT EXISTS sells_24h INTEGER,
            ADD COLUMN IF NOT EXISTS buys_1h INTEGER,
            ADD COLUMN IF NOT EXISTS sells_1h INTEGER,
            ADD COLUMN IF NOT EXISTS buys_5m INTEGER,
            ADD COLUMN IF NOT EXISTS sells_5m INTEGER,
            ADD COLUMN IF NOT EXISTS pair_address TEXT,
            ADD COLUMN IF NOT EXISTS price_updated_at TIMESTAMP
        """)
        conn.commit()
        cur.close()
        log.info("✅ Columnas de precio añadidas")
    finally:
        pool.putconn(conn)

async def fetch_price(session, mint):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            pairs = data.get("pairs")
            if not pairs:
                return mint, None
            # Tomar el par con mayor volumen 24h
            pair = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
            return mint, {
                "price_usd": pair.get("priceUsd"),
                "market_cap": pair.get("marketCap"),
                "volume_24h": pair.get("volume", {}).get("h24"),
                "volume_1h": pair.get("volume", {}).get("h1"),
                "volume_5m": pair.get("volume", {}).get("m5"),
                "buys_24h": pair.get("txns", {}).get("h24", {}).get("buys"),
                "sells_24h": pair.get("txns", {}).get("h24", {}).get("sells"),
                "buys_1h": pair.get("txns", {}).get("h1", {}).get("buys"),
                "sells_1h": pair.get("txns", {}).get("h1", {}).get("sells"),
                "buys_5m": pair.get("txns", {}).get("m5", {}).get("buys"),
                "sells_5m": pair.get("txns", {}).get("m5", {}).get("sells"),
                "pair_address": pair.get("pairAddress"),
            }
    except Exception as e:
        return mint, None

def save_prices(results):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        success = 0
        for mint, data in results:
            if data:
                cur.execute("""
                    UPDATE discovered_tokens SET
                        price_usd = %s,
                        market_cap = %s,
                        volume_24h = %s,
                        volume_1h = %s,
                        volume_5m = %s,
                        buys_24h = %s,
                        sells_24h = %s,
                        buys_1h = %s,
                        sells_1h = %s,
                        buys_5m = %s,
                        sells_5m = %s,
                        pair_address = %s,
                        price_updated_at = NOW()
                    WHERE mint = %s
                """, (
                    data["price_usd"], data["market_cap"],
                    data["volume_24h"], data["volume_1h"], data["volume_5m"],
                    data["buys_24h"], data["sells_24h"],
                    data["buys_1h"], data["sells_1h"],
                    data["buys_5m"], data["sells_5m"],
                    data["pair_address"], mint
                ))
                success += 1
        conn.commit()
        cur.close()
        return success
    finally:
        pool.putconn(conn)

def get_tokens_to_update(limit=100):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint FROM discovered_tokens
            WHERE price_usd IS NOT NULL
            AND created_at > NOW() - INTERVAL '24 hours'
            AND (price_updated_at IS NULL OR price_updated_at < NOW() - INTERVAL '10 minutes')
            ORDER BY created_at DESC
            LIMIT %s
        """, (limit,))
        mints = [row[0] for row in cur.fetchall()]
        cur.close()
        return mints
    finally:
        pool.putconn(conn)

async def main():
    setup_db()
    log.info("💰 Price Fetcher arrancando...")

    while True:
        mints = get_tokens_to_update(100)
        if not mints:
            log.info("Sin tokens para actualizar, esperando 60s...")
            await asyncio.sleep(60)
            continue

        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(5)
            async def fetch_with_sem(mint):
                async with semaphore:
                    return await fetch_price(session, mint)
            results = await asyncio.gather(*[fetch_with_sem(m) for m in mints])

        success = save_prices(results)
        log.info(f"💰 Precios actualizados: {success}/{len(mints)}")
        await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
