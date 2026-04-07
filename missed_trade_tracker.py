"""
missed_trade_tracker.py — One-shot script (timer cada 30min)

Lee missed_trades con seguimiento pendiente y actualiza precios
a los 30min, 1h y 2h desde la señal rechazada.
Calcula pnl_pct y phantom_pnl (referencia $20/trade).

NO modifica paper_trades, paper_capital ni strategy_capital.
"""

import os
import time
import logging
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/missed_trade_tracker.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

PHANTOM_REF_SIZE = 20.0   # $ de referencia para calcular phantom_pnl

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
    )

def fetch_price(mint):
    """Obtiene precio actual desde DexScreener (con fallback a Jupiter)."""
    try:
        url  = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        pairs = data.get("pairs")
        if pairs:
            pair  = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
            price = pair.get("priceUsd")
            if price:
                return float(price)
    except Exception:
        pass

    try:
        url  = f"https://api.jup.ag/price/v2?ids={mint}"
        resp = requests.get(url, timeout=8)
        data = resp.json()
        price_data = data.get("data", {}).get(mint)
        if price_data and price_data.get("price"):
            return float(price_data["price"])
    except Exception:
        pass

    return None

def track():
    conn = get_conn()
    try:
        cur = conn.cursor()
        # Obtener todos los registros con algún intervalo aún sin trackear
        cur.execute("""
            SELECT id, mint, name, symbol, entry_price, missed_at,
                   tracked_30m, tracked_1h, tracked_2h
            FROM missed_trades
            WHERE
                (NOT tracked_30m AND missed_at <= NOW() - INTERVAL '30 minutes')
             OR (NOT tracked_1h  AND missed_at <= NOW() - INTERVAL '1 hour')
             OR (NOT tracked_2h  AND missed_at <= NOW() - INTERVAL '2 hours')
            ORDER BY missed_at DESC
            LIMIT 200
        """)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not rows:
        log.info("Sin missed trades pendientes de tracking")
        return

    log.info(f"Procesando {len(rows)} missed trades...")
    updated = 0

    for row in rows:
        rid, mint, name, symbol, entry_price, missed_at, t30m, t1h, t2h = row
        if not entry_price:
            continue

        elapsed_min = (datetime.now() - missed_at.replace(tzinfo=None)).total_seconds() / 60
        needs_30m   = not t30m and elapsed_min >= 30
        needs_1h    = not t1h  and elapsed_min >= 60
        needs_2h    = not t2h  and elapsed_min >= 120

        if not (needs_30m or needs_1h or needs_2h):
            continue

        current_price = fetch_price(mint)
        time.sleep(0.3)   # rate limit conservador

        if not current_price:
            log.debug(f"Sin precio para {mint[:8]} ({name or '?'})")
            continue

        entry    = float(entry_price)
        pnl_pct  = (current_price - entry) / entry * 100
        phantom  = PHANTOM_REF_SIZE * (pnl_pct / 100)

        conn2 = get_conn()
        try:
            cur2 = conn2.cursor()
            if needs_30m:
                cur2.execute("""
                    UPDATE missed_trades SET
                        price_30m = %s, pnl_pct_30m = %s, phantom_pnl_30m = %s,
                        tracked_30m = TRUE
                    WHERE id = %s AND NOT tracked_30m
                """, (current_price, round(pnl_pct, 2), round(phantom, 2), rid))
            if needs_1h:
                cur2.execute("""
                    UPDATE missed_trades SET
                        price_1h = %s, pnl_pct_1h = %s, phantom_pnl_1h = %s,
                        tracked_1h = TRUE
                    WHERE id = %s AND NOT tracked_1h
                """, (current_price, round(pnl_pct, 2), round(phantom, 2), rid))
            if needs_2h:
                cur2.execute("""
                    UPDATE missed_trades SET
                        price_2h = %s, pnl_pct_2h = %s, phantom_pnl_2h = %s,
                        tracked_2h = TRUE
                    WHERE id = %s AND NOT tracked_2h
                """, (current_price, round(pnl_pct, 2), round(phantom, 2), rid))
            conn2.commit()
            cur2.close()
            updated += 1
            intervals = []
            if needs_30m: intervals.append('30m')
            if needs_1h:  intervals.append('1h')
            if needs_2h:  intervals.append('2h')
            log.info(f"  {name or mint[:8]} | {'+' if pnl_pct > 0 else ''}{pnl_pct:.1f}% | "
                     f"${phantom:+.2f} | intervalos: {', '.join(intervals)}")
        except Exception as e:
            log.warning(f"Update error para {mint[:8]}: {e}")
        finally:
            conn2.close()

    log.info(f"Tracking completo: {updated}/{len(rows)} registros actualizados")

def print_daily_report():
    """Imprime resumen de ayer al log para diagnóstico."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                rejection_reason,
                COUNT(*)                                          as total,
                COUNT(*) FILTER (WHERE tracked_1h)               as tracked,
                ROUND(AVG(pnl_pct_1h) FILTER (WHERE tracked_1h)::numeric, 1) as avg_1h,
                ROUND(SUM(phantom_pnl_1h) FILTER (WHERE tracked_1h)::numeric, 2) as phantom_sum,
                COUNT(*) FILTER (WHERE pnl_pct_1h > 0)           as wins
            FROM missed_trades
            WHERE DATE(missed_at) = CURRENT_DATE - 1
            GROUP BY rejection_reason
            ORDER BY phantom_sum DESC NULLS LAST
        """)
        rows = cur.fetchall()
        cur.close()
        if not rows:
            log.info("Sin missed trades de ayer para reportar")
            return
        log.info("=== POST-MORTEM AYER ===")
        for r in rows:
            reason, total, tracked, avg_1h, phantom, wins = r
            log.info(f"  {reason:<22} | {total} rechazados | "
                     f"{tracked} rastreados | avg={avg_1h or 'N/A'}% | "
                     f"phantom=${phantom or 0:+.2f} | wins={wins or 0}")
    finally:
        conn.close()

if __name__ == "__main__":
    log.info("=== Missed Trade Tracker iniciado ===")
    track()
    print_daily_report()
    log.info("=== Tracker finalizado ===")
