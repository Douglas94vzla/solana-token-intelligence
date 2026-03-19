import asyncio
import aiohttp
import psycopg2
import psycopg2.pool
import os
import logging
import struct
import base64
import time
import requests as _requests
from datetime import datetime
from dotenv import load_dotenv
from solders.pubkey import Pubkey

load_dotenv('/root/solana_bot/.env')

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
        try:
            cur.execute("SET lock_timeout = '5s'")
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
                ADD COLUMN IF NOT EXISTS liquidity_usd NUMERIC(20,2),
                ADD COLUMN IF NOT EXISTS fdv NUMERIC(20,2),
                ADD COLUMN IF NOT EXISTS price_updated_at TIMESTAMP
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"ALTER TABLE skipped (lock timeout): {e}")
        cur.close()
        log.info("✅ Columnas de precio añadidas")
    finally:
        pool.putconn(conn)

# ── BONDING CURVE PRICING ─────────────────────────────
PUMP_PROGRAM     = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
PUBLIC_RPC       = "https://api.mainnet-beta.solana.com"
PUMP_TOTAL_SUPPLY = 1_000_000_000        # 1B tokens total supply
BC_INITIAL_SOL    = 30.0                 # virtual SOL at start (lamports / 1e9)
AVG_BUY_SOL       = 0.1                 # average SOL per buy tx (estimate)

_sol_price_cache  = {"price": 150.0, "ts": 0.0}

def get_sol_price() -> float:
    """Returns cached SOL/USD price, refreshed every 5 minutes."""
    if time.time() - _sol_price_cache["ts"] > 300:
        try:
            r = _requests.get(
                "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd",
                timeout=8
            )
            _sol_price_cache["price"] = r.json()["solana"]["usd"]
            _sol_price_cache["ts"]    = time.time()
        except Exception:
            pass
    return _sol_price_cache["price"]

def get_bonding_curve_address(mint_str: str) -> str:
    mint = Pubkey.from_string(mint_str)
    pda, _ = Pubkey.find_program_address([b"bonding-curve", bytes(mint)], PUMP_PROGRAM)
    return str(pda)

def fetch_bc_prices_batch(mints: list[str]) -> dict[str, dict]:
    """
    Fetches bonding curve account data for up to 100 mints at once.
    Returns {mint: {price_usd, market_cap, buys_5m}} for tokens still in the curve.
    """
    if not mints:
        return {}

    sol_price = get_sol_price()
    bc_addrs  = [get_bonding_curve_address(m) for m in mints]

    results = {}
    # Public RPC only — Helius is reserved for main data
    for start in range(0, len(bc_addrs), 100):
        batch_mints = mints[start:start+100]
        batch_addrs = bc_addrs[start:start+100]
        try:
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getMultipleAccounts",
                "params": [batch_addrs, {"encoding": "base64"}]
            }
            resp = _requests.post(PUBLIC_RPC, json=payload, timeout=15).json()
            values = resp.get("result", {}).get("value", [])
            for mint, val in zip(batch_mints, values):
                if not val:
                    continue
                data = base64.b64decode(val["data"][0])
                if len(data) < 49:
                    continue
                vt       = struct.unpack_from('<Q', data, 8)[0]   # virtual token reserves
                vs       = struct.unpack_from('<Q', data, 16)[0]  # virtual sol reserves (lamports)
                rs       = struct.unpack_from('<Q', data, 32)[0]  # real sol reserves (lamports)
                complete = bool(data[48])
                if complete or vt == 0:
                    continue  # graduated — DexScreener will have it

                vs_sol       = vs / 1e9
                rs_sol       = rs / 1e9  # actual SOL from buyers
                price_sol    = vs_sol / (vt / 1e6)  # SOL per token (vt in base units)
                price_usd    = price_sol * sol_price
                market_cap   = price_usd * PUMP_TOTAL_SUPPLY

                # Estimate buys from real SOL raised (avg 0.1 SOL per buy)
                est_buys = max(0, min(100, round(rs_sol / AVG_BUY_SOL)))

                results[mint] = {
                    "price_usd":  round(price_usd, 10),
                    "market_cap": round(market_cap, 2),
                    "buys_5m":    est_buys,
                    "sells_5m":   0,  # conservative — can't distinguish from BC
                }
        except Exception as e:
            log.debug(f"BC batch error: {e}")

    return results

def get_tokens_without_price(limit=200) -> list[str]:
    """Tokens created in last 3h with no DexScreener price yet."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint FROM discovered_tokens
            WHERE price_usd IS NULL
              AND created_at > NOW() - INTERVAL '3 hours'
              AND mint LIKE '%%pump'
            ORDER BY created_at DESC
            LIMIT %s
        """, (limit,))
        mints = [r[0] for r in cur.fetchall()]
        cur.close()
        return mints
    finally:
        pool.putconn(conn)

def save_bc_prices(bc_data: dict[str, dict]):
    """Saves bonding curve prices for tokens that still lack DexScreener data."""
    if not bc_data:
        return 0
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        saved = 0
        for mint, d in bc_data.items():
            cur.execute("""
                UPDATE discovered_tokens SET
                    price_usd        = %s,
                    market_cap       = %s,
                    buys_5m          = COALESCE(buys_5m, %s),
                    sells_5m         = COALESCE(sells_5m, %s),
                    price_updated_at = NOW()
                WHERE mint = %s AND price_usd IS NULL
            """, (d["price_usd"], d["market_cap"],
                  d["buys_5m"], d["sells_5m"], mint))
            if cur.rowcount > 0:
                saved += 1
        conn.commit()
        cur.close()
        return saved
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
            pc = pair.get("priceChange") or {}
            return mint, {
                "price_usd":        pair.get("priceUsd"),
                "market_cap":       pair.get("marketCap"),
                "volume_24h":       pair.get("volume", {}).get("h24"),
                "volume_1h":        pair.get("volume", {}).get("h1"),
                "volume_5m":        pair.get("volume", {}).get("m5"),
                "buys_24h":         pair.get("txns", {}).get("h24", {}).get("buys"),
                "sells_24h":        pair.get("txns", {}).get("h24", {}).get("sells"),
                "buys_1h":          pair.get("txns", {}).get("h1", {}).get("buys"),
                "sells_1h":         pair.get("txns", {}).get("h1", {}).get("sells"),
                "buys_5m":          pair.get("txns", {}).get("m5", {}).get("buys"),
                "sells_5m":         pair.get("txns", {}).get("m5", {}).get("sells"),
                "pair_address":     pair.get("pairAddress"),
                "liquidity_usd":    pair.get("liquidity", {}).get("usd"),
                "fdv":              pair.get("fdv"),
                "price_change_15m": pc.get("m5"),   # DexScreener no tiene 15m, usamos 5m
                "price_change_1h":  pc.get("h1"),
                "price_change_6h":  pc.get("h6"),
                "price_change_24h": pc.get("h24"),
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
                        price_usd        = %s,
                        market_cap       = %s,
                        volume_24h       = %s,
                        volume_1h        = %s,
                        volume_5m        = %s,
                        buys_24h         = %s,
                        sells_24h        = %s,
                        buys_1h          = %s,
                        sells_1h         = %s,
                        buys_5m          = %s,
                        sells_5m         = %s,
                        pair_address     = %s,
                        liquidity_usd    = COALESCE(%s, liquidity_usd),
                        fdv              = COALESCE(%s, fdv),
                        price_change_15m = COALESCE(%s, price_change_15m),
                        price_change_1h  = COALESCE(%s, price_change_1h),
                        price_change_6h  = %s,
                        price_change_24h = %s,
                        price_updated_at = NOW()
                    WHERE mint = %s
                """, (
                    data["price_usd"], data["market_cap"],
                    data["volume_24h"], data["volume_1h"], data["volume_5m"],
                    data["buys_24h"], data["sells_24h"],
                    data["buys_1h"], data["sells_1h"],
                    data["buys_5m"], data["sells_5m"],
                    data["pair_address"],
                    data["liquidity_usd"], data["fdv"],
                    data["price_change_15m"], data["price_change_1h"],
                    data["price_change_6h"], data["price_change_24h"],
                    mint
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
        log.info(f"💰 Precios DexScreener: {success}/{len(mints)}")

        # ── BONDING CURVE para tokens sin precio DexScreener ──
        bc_mints = get_tokens_without_price(200)
        if bc_mints:
            bc_data = fetch_bc_prices_batch(bc_mints)
            bc_saved = save_bc_prices(bc_data)
            if bc_saved:
                log.info(f"⛓️  Bonding curve: {bc_saved}/{len(bc_mints)} tokens con precio inicial")

        await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
