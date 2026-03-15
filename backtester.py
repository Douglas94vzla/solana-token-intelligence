import psycopg2
import psycopg2.pool
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/backtester.log'),
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

# ── PARÁMETROS — alineados con paper_trading.py ───────
CAPITAL          = 1000.0
TRADE_SIZE_PCT   = 0.02      # 2% por trade (Kelly medio)
TAKE_PROFIT      = 1.50      # +50%
STOP_LOSS        = 0.70      # -30%
TRAIL_PCT        = 0.20      # Trailing: -20% desde pico
TRAIL_ACTIVATE   = 0.10      # Trailing activa tras +10%
MAX_HOLD_MINUTES = 120
SLIPPAGE         = 0.03
FEES             = 0.005
ML_THRESHOLD     = 65.0      # Misma que paper_trading

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS backtest_results (
                id           SERIAL PRIMARY KEY,
                run_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                n_trades     INTEGER,
                wins         INTEGER,
                losses       INTEGER,
                win_rate     NUMERIC(5,2),
                total_pnl    NUMERIC(20,2),
                roi_pct      NUMERIC(15,2),
                avg_pnl_pct  NUMERIC(15,2),
                best_pct     NUMERIC(15,2),
                worst_pct    NUMERIC(15,2),
                filter_mode  TEXT
            )
        """)
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_candidates():
    """Tokens con ML probability >= threshold y suficientes snapshots."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                dt.mint, dt.name, dt.symbol,
                dt.price_usd, dt.market_cap,
                dt.buys_5m, dt.sells_5m,
                dt.volume_24h, dt.created_at,
                dt.ml_probability, dt.narrative,
                dt.rug_score, dt.survival_score
            FROM discovered_tokens dt
            WHERE dt.price_usd IS NOT NULL
              AND dt.survival_score IS NOT NULL
              AND dt.ml_probability >= %s
              AND (dt.rug_score IS NULL OR dt.rug_score < 60)
              AND dt.mint IN (
                  SELECT mint FROM price_snapshots
                  GROUP BY mint HAVING COUNT(*) >= 10
              )
            ORDER BY dt.created_at DESC
            LIMIT 500
        """, (ML_THRESHOLD,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def get_price_evolution(mint, entry_time):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT price_usd, snapshot_at
            FROM price_snapshots
            WHERE mint = %s AND snapshot_at > %s AND price_usd > 0
            ORDER BY snapshot_at ASC
        """, (mint, entry_time))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def simulate_trade(token, capital):
    mint, name, symbol, entry_price, entry_mcap, buys, sells, volume, \
        created_at, ml_prob, narrative, rug_score, score = token

    if not entry_price or float(entry_price) <= 0:
        return None

    entry  = float(entry_price) * (1 + SLIPPAGE + FEES)
    size   = round(capital * TRADE_SIZE_PCT, 2)
    peak   = entry

    snapshots = get_price_evolution(mint, created_at)

    exit_price  = entry
    exit_reason = 'TIMEOUT'
    hold_min    = MAX_HOLD_MINUTES

    for snap_price, snap_time in snapshots:
        curr  = float(snap_price)
        mins  = (snap_time - created_at).total_seconds() / 60

        if mins > MAX_HOLD_MINUTES:
            exit_price  = curr
            exit_reason = 'TIMEOUT'
            hold_min    = mins
            break

        if curr > peak:
            peak = curr

        # Take profit — usar precio exacto de TP, no el snapshot (evita overshoot)
        if curr >= entry * TAKE_PROFIT:
            exit_price  = entry * TAKE_PROFIT
            exit_reason = 'TAKE_PROFIT'
            hold_min    = mins
            break

        # Trailing stop
        trail_price = peak * (1 - TRAIL_PCT)
        if peak >= entry * (1 + TRAIL_ACTIVATE) and curr <= trail_price:
            exit_price  = trail_price
            exit_reason = 'TRAILING_STOP'
            hold_min    = mins
            break

        # Stop loss — usar precio exacto de SL
        if curr <= entry * STOP_LOSS:
            exit_price  = entry * STOP_LOSS
            exit_reason = 'STOP_LOSS'
            hold_min    = mins
            break

        exit_price = curr
        hold_min   = mins

    exit_val = exit_price * (1 - FEES)
    pnl_pct  = (exit_val - entry) / entry * 100
    pnl      = size * (pnl_pct / 100)

    return {
        'mint':        mint,
        'name':        name or mint[:8],
        'symbol':      symbol or '???',
        'narrative':   narrative or 'OTHER',
        'ml_prob':     ml_prob,
        'pnl':         pnl,
        'pnl_pct':     pnl_pct,
        'exit_reason': exit_reason,
        'hold_min':    hold_min,
        'size':        size,
    }

def save_backtest_results(trades, capital_final, filter_mode='ML>=65+RUG<60'):
    if not trades:
        return
    wins    = sum(1 for t in trades if t['pnl'] > 0)
    losses  = len(trades) - wins
    wr      = wins / len(trades) * 100
    total   = sum(t['pnl'] for t in trades)
    roi     = (capital_final - CAPITAL) / CAPITAL * 100
    avg_pct = sum(t['pnl_pct'] for t in trades) / len(trades)
    best    = max(t['pnl_pct'] for t in trades)
    worst   = min(t['pnl_pct'] for t in trades)

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO backtest_results
            (n_trades, wins, losses, win_rate, total_pnl, roi_pct,
             avg_pnl_pct, best_pct, worst_pct, filter_mode)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (len(trades), wins, losses, round(wr, 2), round(total, 2),
              round(roi, 2), round(avg_pct, 2), round(best, 2),
              round(worst, 2), filter_mode))
        conn.commit()
        cur.close()
        log.info(f"📊 Backtest guardado: {len(trades)} trades | WR={wr:.1f}% | ROI={roi:+.1f}%")
    finally:
        pool.putconn(conn)

def compare_with_paper():
    """Muestra diferencia entre backtest y paper trading real."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()

        # Últimos 2 backtests
        cur.execute("""
            SELECT run_at, n_trades, win_rate, roi_pct, avg_pnl_pct
            FROM backtest_results ORDER BY run_at DESC LIMIT 2
        """)
        bt = cur.fetchall()

        # Paper trading real
        cur.execute("""
            SELECT COUNT(*),
                   COUNT(CASE WHEN pnl > 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*),0),
                   AVG(pnl_pct)
            FROM paper_trades WHERE status = 'CLOSED'
        """)
        pt = cur.fetchone()
        cur.close()

        print("\n" + "="*60)
        print("📊 BACKTEST vs PAPER TRADING REAL")
        print("="*60)
        if bt:
            b = bt[0]
            print(f"  Backtest ({str(b[0])[:16]}): {b[1]} trades | WR={b[2]:.1f}% | ROI={b[3]:+.1f}%")
        if pt and pt[0]:
            print(f"  Paper Trading real:       {pt[0]} trades | WR={pt[1]:.1f}% | AvgPnL={pt[2]:+.1f}%")
        print("="*60 + "\n")
    finally:
        pool.putconn(conn)

def run_backtest():
    setup_db()
    candidates = get_candidates()
    log.info(f"🔬 Backtesting {len(candidates)} tokens (ML≥{ML_THRESHOLD}%, rug<60)...")

    if not candidates:
        log.warning("Sin candidatos con suficientes snapshots")
        return []

    capital = CAPITAL
    trades  = []

    for token in candidates:
        result = simulate_trade(token, capital)
        if not result:
            continue
        capital += result['pnl']
        trades.append(result)

    if not trades:
        log.warning("Sin trades simulados")
        return []

    wins   = sum(1 for t in trades if t['pnl'] > 0)
    losses = len(trades) - wins
    wr     = wins / len(trades) * 100
    roi    = (capital - CAPITAL) / CAPITAL * 100

    # Resumen por narrativa
    by_narr = {}
    for t in trades:
        n = t['narrative']
        if n not in by_narr:
            by_narr[n] = {'wins': 0, 'total': 0, 'pnl': 0}
        by_narr[n]['total'] += 1
        by_narr[n]['pnl']   += t['pnl']
        if t['pnl'] > 0:
            by_narr[n]['wins'] += 1

    reasons = {}
    for t in trades:
        reasons[t['exit_reason']] = reasons.get(t['exit_reason'], 0) + 1

    print("\n" + "="*65)
    print(f"🔬 BACKTEST REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   Filtros: ML≥{ML_THRESHOLD}% | Rug<60 | TP={TAKE_PROFIT}x | SL={STOP_LOSS}x | Trail={TRAIL_PCT*100:.0f}%")
    print("="*65)
    print(f"  Capital:   ${CAPITAL:.0f} → ${capital:.2f}  ({roi:+.1f}%)")
    print(f"  Trades:    {len(trades)} | Wins: {wins} | Losses: {losses} | WR: {wr:.1f}%")
    avg = sum(t['pnl_pct'] for t in trades) / len(trades)
    best  = max(trades, key=lambda x: x['pnl_pct'])
    worst = min(trades, key=lambda x: x['pnl_pct'])
    print(f"  AvgPnL:    {avg:+.1f}% | Best: {best['pnl_pct']:+.1f}% ({best['name']}) | Worst: {worst['pnl_pct']:+.1f}%")
    print()
    print("  SALIDAS:")
    for r, c in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"    {r:<20} {c} trades")
    print()
    print("  TOP NARRATIVAS:")
    for narr, stats in sorted(by_narr.items(), key=lambda x: -x[1]['pnl'])[:6]:
        wr_n = stats['wins'] / stats['total'] * 100
        print(f"    {narr:<20} {stats['total']:>3} trades | WR={wr_n:.0f}% | PnL=${stats['pnl']:+.2f}")
    print("="*65 + "\n")

    save_backtest_results(trades, capital)
    compare_with_paper()
    return trades

if __name__ == "__main__":
    log.info("🔬 Backtester iniciando...")
    run_backtest()
    log.info("✅ Backtest completado")
