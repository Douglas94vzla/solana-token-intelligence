"""
seed_smart_wallets.py — One-shot smart wallet seeder.

Seeds smart_wallets table by finding wallets that appear as early buyers
in multiple winning paper trades, then validating their win rate against
losing trades.

Run once manually or via cron. Safe to re-run (idempotent).

Usage:
    python seed_smart_wallets.py
"""

import requests
import psycopg2
import psycopg2.pool
import os
import time
import logging
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/smart_money.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

RPC_URL    = os.getenv("RPC_URL")
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"

# Minimum wins to be considered smart
MIN_WINS       = 2
MIN_WIN_RATE   = 0.55   # 55%
SIGS_PER_TOKEN = 10     # transactions to sample per token (reduced for speed)
DELAY_BETWEEN  = 0.5    # seconds between RPC calls

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)


def get_paper_trade_outcomes():
    """
    Returns (mint, created_at, won, pnl_pct) for closed paper trades.
    Prefers recent (last 7 days) since older token data is pruned from public RPC.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT pt.mint, dt.created_at, (pt.pnl_pct > 0) AS won, pt.pnl_pct
            FROM paper_trades pt
            JOIN discovered_tokens dt ON dt.mint = pt.mint
            WHERE pt.status = 'CLOSED'
            ORDER BY dt.created_at DESC   -- recent first (more likely in RPC)
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)


def get_recent_strong_signals():
    """
    Returns recent tokens (last 3 days) with strong momentum as additional
    'winners' — tokens with high price_change_1h are good proxies.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, created_at
            FROM discovered_tokens
            WHERE signal IN ('STRONG_BUY', 'BUY')
            AND price_change_1h > 50
            AND created_at > NOW() - INTERVAL '3 days'
            AND buys_5m > 5
            ORDER BY price_change_1h DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)


def get_signatures(mint: str, limit: int = SIGS_PER_TOKEN) -> list[dict]:
    """Gets recent signatures for a mint. Falls back to public RPC."""
    for rpc in [RPC_URL, PUBLIC_RPC]:
        try:
            resp = requests.post(rpc, json={
                "jsonrpc": "2.0", "id": 1,
                "method": "getSignaturesForAddress",
                "params": [mint, {"limit": limit}]
            }, timeout=12).json()
            if resp.get("error"):
                log.debug(f"getSignaturesForAddress error on {rpc[:30]}: {resp['error']}")
                time.sleep(2)
                continue
            return resp.get("result", [])
        except Exception as e:
            log.debug(f"get_signatures exception: {e}")
    return []


def get_signer(signature: str) -> str | None:
    """Extracts the fee payer / signer from a transaction."""
    for rpc in [RPC_URL, PUBLIC_RPC]:
        try:
            resp = requests.post(rpc, json={
                "jsonrpc": "2.0", "id": 1,
                "method": "getTransaction",
                "params": [signature, {
                    "encoding": "jsonParsed",
                    "maxSupportedTransactionVersion": 0
                }]
            }, timeout=12).json()
            if resp.get("error"):
                time.sleep(1)
                continue
            result = resp.get("result")
            if not result:
                return None
            # Fee payer is always the first account key with signer=True
            keys = result.get("transaction", {}).get("message", {}).get("accountKeys", [])
            for k in keys:
                if isinstance(k, dict) and k.get("signer") and k.get("writable"):
                    addr = k.get("pubkey", "")
                    if len(addr) > 30:
                        return addr
            return None
        except Exception:
            pass
    return None


def extract_buyers(mint: str, limit: int = SIGS_PER_TOKEN) -> set[str]:
    """
    Gets the first `limit` signers for a mint.
    Note: returns RECENT signers (last N txs), not necessarily early buyers.
    With only 12 winners, this is a useful proxy — repeat signers across
    multiple winners are likely genuine smart money.
    """
    buyers = set()
    sigs = get_signatures(mint, limit=limit)
    log.info(f"  {mint[:12]}... → {len(sigs)} signatures")

    for sig_info in sigs:
        sig = sig_info.get("signature", "")
        if not sig:
            continue
        signer = get_signer(sig)
        if signer and not signer.startswith("11111111"):
            buyers.add(signer)
        time.sleep(0.15)  # Rate limit

    return buyers


def update_smart_wallet(cur, wallet: str, won: bool):
    """Upserts smart_wallets table with a win or loss."""
    cur.execute("""
        INSERT INTO smart_wallets (wallet, wins, losses, total_trades, last_seen)
        VALUES (%s, %s, %s, 1, NOW())
        ON CONFLICT (wallet) DO UPDATE SET
            wins         = smart_wallets.wins   + %s,
            losses       = smart_wallets.losses + %s,
            total_trades = smart_wallets.total_trades + 1,
            last_seen    = NOW()
    """, (
        wallet,
        1 if won else 0,
        0 if won else 1,
        1 if won else 0,
        0 if won else 1
    ))
    cur.execute("""
        UPDATE smart_wallets
        SET win_rate = ROUND(wins::numeric / NULLIF(total_trades,0) * 100, 2),
            is_smart = (
                wins::numeric / NULLIF(total_trades,0) >= %s
                AND total_trades >= %s
            )
        WHERE wallet = %s
    """, (MIN_WIN_RATE, MIN_WINS, wallet))


def save_wallet_activity(cur, wallet: str, mint: str, sig: str):
    cur.execute("""
        INSERT INTO wallet_activity (wallet, mint, action, signature)
        VALUES (%s, %s, 'BUY', %s)
        ON CONFLICT (wallet, mint, action) DO NOTHING
    """, (wallet, mint, sig))


def reset_seeded_wallets():
    """
    Resets wins/losses/is_smart for wallets seeded by the old approach
    (all had 100% win rate because only winners were analyzed).
    Wallets with last_seen before 2026-03-11 are from old seeding.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE smart_wallets
            SET wins=0, losses=0, total_trades=0, win_rate=0, is_smart=FALSE
            WHERE last_seen < '2026-03-11'
        """)
        affected = cur.rowcount
        conn.commit()
        cur.close()
        log.info(f"🔄 Reset {affected} stale wallets from old seeding (pre-Mar-11)")
    finally:
        pool.putconn(conn)


def run():
    log.info("🧠 Smart Wallet Seeder iniciando...")

    # Step 1: Reset stale wallets from old biased seeding
    reset_seeded_wallets()

    # Step 2: Get all closed paper trades
    trades = get_paper_trade_outcomes()
    winners = [(m, c, pnl) for m, c, won, pnl in trades if won]
    losers  = [(m, c, pnl) for m, c, won, pnl in trades if not won]
    log.info(f"📊 Paper trades: {len(winners)} ganadores | {len(losers)} perdedores")

    if not winners:
        log.warning("Sin paper trades ganadores — espera a que haya más trades cerrados")
        return

    # Step 2b: Add recent strong-signal tokens as extra winners
    recent = get_recent_strong_signals()
    log.info(f"📈 Recent strong-signal tokens (last 3 days): {len(recent)}")
    # Merge: (mint, created_at, pnl=999 as placeholder)
    winners_extended = [(m, c, pnl) for m, c, won, pnl in trades if won]
    for mint, created_at in recent:
        if not any(m == mint for m, _, _ in winners_extended):
            winners_extended.append((mint, created_at, 999))
    log.info(f"🏆 Total tokens ganadores (paper + recent signals): {len(winners_extended)}")

    # Step 3: Collect buyers from winning tokens
    log.info("🏆 Analizando tokens ganadores...")
    winner_buyers: dict[str, set[str]] = {}   # mint → set of buyer wallets
    all_sigs_by_wallet: dict[str, dict[str, str]] = defaultdict(dict)  # wallet → {mint: sig}

    for mint, created_at, pnl in winners_extended:
        log.info(f"  🟢 {mint[:12]}... pnl=+{pnl:.0f}%")
        sigs = get_signatures(mint, limit=SIGS_PER_TOKEN)
        buyers = set()
        for sig_info in sigs:
            sig = sig_info.get("signature", "")
            signer = get_signer(sig)
            if signer and not signer.startswith("11111111"):
                buyers.add(signer)
                all_sigs_by_wallet[signer][mint] = sig
            time.sleep(0.15)
        winner_buyers[mint] = buyers
        log.info(f"    → {len(buyers)} wallets únicas")
        time.sleep(DELAY_BETWEEN)

    # Step 4: Collect buyers from losing tokens
    log.info("💀 Analizando tokens perdedores...")
    loser_buyers: dict[str, set[str]] = {}

    for mint, created_at, pnl in losers:
        log.info(f"  🔴 {mint[:12]}... pnl={pnl:.0f}%")
        sigs = get_signatures(mint, limit=SIGS_PER_TOKEN)
        buyers = set()
        for sig_info in sigs:
            sig = sig_info.get("signature", "")
            signer = get_signer(sig)
            if signer and not signer.startswith("11111111"):
                buyers.add(signer)
        loser_buyers[mint] = buyers
        log.info(f"    → {len(buyers)} wallets únicas")
        time.sleep(DELAY_BETWEEN)

    # Step 5: Compute per-wallet wins and losses
    wallet_wins   = defaultdict(int)
    wallet_losses = defaultdict(int)

    for mint, buyers in winner_buyers.items():
        for w in buyers:
            wallet_wins[w] += 1

    for mint, buyers in loser_buyers.items():
        for w in buyers:
            wallet_losses[w] += 1

    # Step 6: Update DB
    conn = pool.getconn()
    try:
        cur = conn.cursor()

        all_wallets = set(wallet_wins.keys()) | set(wallet_losses.keys())
        log.info(f"💾 Guardando {len(all_wallets)} wallets...")

        for wallet in all_wallets:
            wins   = wallet_wins[wallet]
            losses = wallet_losses[wallet]
            total  = wins + losses
            wr     = wins / total if total else 0

            # Bulk update wins and losses
            cur.execute("""
                INSERT INTO smart_wallets (wallet, wins, losses, total_trades, last_seen)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (wallet) DO UPDATE SET
                    wins         = EXCLUDED.wins,
                    losses       = EXCLUDED.losses,
                    total_trades = EXCLUDED.total_trades,
                    last_seen    = NOW(),
                    win_rate     = ROUND(EXCLUDED.wins::numeric / NULLIF(EXCLUDED.total_trades,0) * 100, 2),
                    is_smart     = (
                        EXCLUDED.wins::numeric / NULLIF(EXCLUDED.total_trades,0) >= %s
                        AND EXCLUDED.total_trades >= %s
                    )
            """, (wallet, wins, losses, total, MIN_WIN_RATE, MIN_WINS))

            # Record wallet_activity for winners
            for mint, sig in all_sigs_by_wallet.get(wallet, {}).items():
                save_wallet_activity(cur, wallet, mint, sig)

        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

    # Step 7: Summary
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT wallet, wins, losses, total_trades, win_rate
            FROM smart_wallets
            WHERE is_smart = TRUE
            ORDER BY wins DESC, win_rate DESC
            LIMIT 20
        """)
        smart = cur.fetchall()
        cur.close()
    finally:
        pool.putconn(conn)

    log.info(f"\n{'='*60}")
    log.info(f"🧠 SMART WALLETS IDENTIFICADAS: {len(smart)}")
    log.info(f"{'='*60}")
    for wallet, wins, losses, total, wr in smart:
        log.info(f"  {wallet[:20]}... | {wins}W/{losses}L | {wr:.0f}% win rate")
    log.info(f"{'='*60}")
    log.info("✅ Seeding completado")


if __name__ == "__main__":
    run()
