"""
wallet_watcher.py — Smart wallet activity monitor via HTTP polling.

Polls getSignaturesForAddress every 60s for each tracked smart wallet.
Detects when a smart wallet buys a pump.fun token and records it
in wallet_activity so entry_signal.py and survival_scorer.py can
use smart_money_bought as an ML feature.

Previous approach (one WebSocket per wallet) was broken since day 1:
- 15 concurrent WS connections → HTTP 429 from Helius immediately.
- smart_money_bought = 0 for ALL 804 token_features_at_signal records.
"""

import requests
import psycopg2
import psycopg2.pool
import os
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/wallet_watcher.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

RPC_URL    = os.getenv("RPC_URL")
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"

POLL_INTERVAL  = 60    # seconds between polling cycles
MAX_WALLETS    = 10    # top N smart wallets to track
SIGS_PER_POLL  = 20    # signatures to check per wallet per cycle
PUMP_SUFFIX    = "pump"

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

# In-memory: last seen signature per wallet (avoids re-processing old txs)
last_seen_sig: dict[str, str] = {}


def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallet_alerts (
                id         SERIAL PRIMARY KEY,
                wallet     TEXT NOT NULL,
                mint       TEXT,
                action     TEXT,
                mcap       NUMERIC,
                signature  TEXT,
                alerted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        log.warning(f"setup_db: {e}")
    finally:
        pool.putconn(conn)


def get_smart_wallets() -> list[tuple]:
    """Returns top MAX_WALLETS smart wallets ordered by wins."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT wallet, wins, win_rate
            FROM smart_wallets
            WHERE is_smart = TRUE AND wins >= 3
            ORDER BY wins DESC, win_rate DESC
            LIMIT %s
        """, (MAX_WALLETS,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)


def get_new_signatures(wallet: str, limit: int = SIGS_PER_POLL) -> list[dict]:
    """
    Polls getSignaturesForAddress for a wallet.
    Returns only signatures newer than last_seen_sig[wallet].
    Uses Helius first, falls back to public RPC.
    """
    for rpc in [RPC_URL, PUBLIC_RPC]:
        try:
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getSignaturesForAddress",
                "params": [wallet, {"limit": limit}]
            }
            resp = requests.post(rpc, json=payload, timeout=10).json()
            if resp.get("error"):
                continue
            sigs = resp.get("result", [])
            if not sigs:
                return []

            # Filter: only signatures we haven't seen yet
            prev = last_seen_sig.get(wallet)
            new_sigs = []
            for s in sigs:
                if prev and s["signature"] == prev:
                    break
                new_sigs.append(s)

            # Update cursor for next poll
            last_seen_sig[wallet] = sigs[0]["signature"]
            return new_sigs
        except Exception as e:
            log.debug(f"get_new_signatures [{rpc[:30]}] error: {e}")

    return []


def get_pump_mint_from_tx(signature: str) -> str | None:
    """
    Fetches a transaction and extracts the pump.fun mint address.
    Checks both instructions and inner instructions.
    """
    for rpc in [RPC_URL, PUBLIC_RPC]:
        try:
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getTransaction",
                "params": [signature, {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0
                }]
            }
            resp = requests.post(rpc, json=payload, timeout=10).json()
            if resp.get("error"):
                continue
            result = resp.get("result")
            if not result:
                return None

            # Check top-level instructions
            for ix in result.get("transaction", {}).get("message", {}).get("instructions", []):
                mint = ix.get("parsed", {}).get("info", {}).get("mint", "")
                if mint.endswith(PUMP_SUFFIX):
                    return mint

            # Check inner instructions
            for group in result.get("meta", {}).get("innerInstructions", []):
                for ix in group.get("instructions", []):
                    mint = ix.get("parsed", {}).get("info", {}).get("mint", "")
                    if mint.endswith(PUMP_SUFFIX):
                        return mint

            # Also check account keys for pump tokens
            keys = result.get("transaction", {}).get("message", {}).get("accountKeys", [])
            for k in keys:
                addr = k.get("pubkey", "") if isinstance(k, dict) else str(k)
                if addr.endswith(PUMP_SUFFIX):
                    return addr

            return None
        except Exception:
            pass

    return None


def is_known_token(mint: str) -> bool:
    """Returns True if this mint is in our discovered_tokens."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM discovered_tokens WHERE mint=%s", (mint,))
        result = cur.fetchone()
        cur.close()
        return result is not None
    finally:
        pool.putconn(conn)


def record_wallet_activity(wallet: str, mint: str, slot: int, signature: str):
    """Inserts into wallet_activity (ON CONFLICT DO NOTHING)."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO wallet_activity (wallet, mint, action, slot, signature)
            VALUES (%s, %s, 'BUY', %s, %s)
            ON CONFLICT (wallet, mint, action) DO NOTHING
        """, (wallet, mint, slot, signature))
        conn.commit()
        if cur.rowcount > 0:
            log.info(f"🧠 Smart wallet {wallet[:12]}... bought {mint[:12]}... | slot={slot}")
        cur.close()
    except Exception as e:
        log.warning(f"record_wallet_activity error: {e}")
    finally:
        pool.putconn(conn)


def poll_wallet(wallet: str, wins: int, win_rate: float):
    """Single poll cycle for one wallet."""
    new_sigs = get_new_signatures(wallet)
    if not new_sigs:
        return

    for sig_info in new_sigs:
        sig  = sig_info.get("signature", "")
        slot = sig_info.get("slot", 0)
        err  = sig_info.get("err")
        if err:
            continue  # Failed transaction — skip

        mint = get_pump_mint_from_tx(sig)
        if not mint:
            continue

        # Only log activity for tokens we're already tracking
        if is_known_token(mint):
            record_wallet_activity(wallet, mint, slot, sig)

        time.sleep(0.5)  # Rate limit between getTransaction calls


def main():
    setup_db()
    log.info("🧠 Wallet Watcher iniciando (modo polling HTTP)...")

    wallets = get_smart_wallets()
    if not wallets:
        log.warning("Sin smart wallets con is_smart=TRUE y wins>=3. "
                    "Corre smart_money.py para poblar smart_wallets.")
        # Still run the loop so it auto-picks up new wallets
        wallets = []

    log.info(f"🎯 Monitoreando {len(wallets)} smart wallets (ciclo: {POLL_INTERVAL}s)")
    for w, wins, wr in wallets:
        log.info(f"  {w[:20]}... | wins={wins} | win_rate={wr:.0f}%")

    cycle = 0
    while True:
        cycle += 1

        # Reload smart wallets every 10 cycles (10 min) so new ones are picked up
        if cycle % 10 == 1:
            wallets = get_smart_wallets()

        for wallet, wins, win_rate in wallets:
            try:
                poll_wallet(wallet, wins, win_rate)
            except Exception as e:
                log.warning(f"poll_wallet {wallet[:12]}: {e}")
            time.sleep(1)  # 1s between wallet requests

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
