import psycopg2
import psycopg2.pool
import os
import logging
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from telegram_bot import (alert_paper_trade, alert_daily_summary,
                           alert_system_status, send_message)

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/paper_trading.log'),
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

# ── RISK MANAGEMENT ───────────────────────────────────
INITIAL_CAPITAL  = 1000.0   # Capital simulado
TRADE_SIZE_PCT   = 0.02     # 2% por trade
MAX_OPEN_TRADES  = 3        # Máximo trades simultáneos
TAKE_PROFIT      = 1.50     # +50% TP
STOP_LOSS        = 0.70     # -30% stop inicial fijo
TRAIL_PCT        = 0.20     # Trailing: cierra si cae 20% desde el pico
TRAIL_ACTIVATE   = 0.10     # Trailing activa solo si el trade subió 10%+
MAX_HOLD_MINUTES = 120      # Timeout 2 horas
DAILY_LOSS_LIMIT = 0.03     # Parar si perdemos 3% en el día
SLIPPAGE         = 0.03     # 3% slippage simulado
FEES             = 0.005    # 0.5% fees
TRADE_HOURS_UTC  = (13, 23) # Solo abrir trades entre 13h y 23h UTC

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id SERIAL PRIMARY KEY,
                mint TEXT NOT NULL,
                name TEXT,
                symbol TEXT,
                entry_price NUMERIC(20,10),
                exit_price NUMERIC(20,10),
                peak_price NUMERIC(20,10),
                trade_size NUMERIC(20,2),
                pnl NUMERIC(20,2),
                pnl_pct NUMERIC(10,2),
                ml_probability NUMERIC(5,2),
                survival_score INTEGER,
                narrative TEXT,
                status TEXT DEFAULT 'OPEN',
                exit_reason TEXT,
                opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS paper_capital (
                id SERIAL PRIMARY KEY,
                capital NUMERIC(20,2) DEFAULT 1000.0,
                daily_pnl NUMERIC(20,2) DEFAULT 0,
                total_pnl NUMERIC(20,2) DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            INSERT INTO paper_capital (capital)
            SELECT 1000.0 WHERE NOT EXISTS (SELECT 1 FROM paper_capital);
        """)
        conn.commit()
        # Añadir peak_price si no existe (migración)
        try:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS peak_price NUMERIC(20,10)")
            conn.commit()
        except Exception:
            conn.rollback()
        cur.close()
        log.info("✅ Tablas paper trading creadas")
    finally:
        pool.putconn(conn)

def get_capital():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT capital, daily_pnl, total_pnl, wins, losses FROM paper_capital LIMIT 1")
        row = cur.fetchone()
        cur.close()
        return {
            'capital':   float(row[0]),
            'daily_pnl': float(row[1]),
            'total_pnl': float(row[2]),
            'wins':      row[3],
            'losses':    row[4]
        }
    finally:
        pool.putconn(conn)

def update_capital(capital, daily_pnl, total_pnl, wins, losses):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE paper_capital SET
                capital = %s, daily_pnl = %s, total_pnl = %s,
                wins = %s, losses = %s, updated_at = NOW()
        """, (capital, daily_pnl, total_pnl, wins, losses))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_open_trades():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, mint, name, symbol, entry_price, trade_size,
                   ml_probability, survival_score, opened_at,
                   COALESCE(peak_price, entry_price) as peak_price
            FROM paper_trades
            WHERE status = 'OPEN'
            ORDER BY opened_at ASC
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def update_peak_price(trade_id, peak_price):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE paper_trades SET peak_price = %s WHERE id = %s",
                    (peak_price, trade_id))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def open_trade(mint, name, symbol, price, ml_prob, score, narrative, capital):
    trade_size  = round(capital * TRADE_SIZE_PCT, 2)
    entry_price = float(price) * (1 + SLIPPAGE + FEES)

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO paper_trades
            (mint, name, symbol, entry_price, peak_price, trade_size,
             ml_probability, survival_score, narrative, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'OPEN')
            RETURNING id
        """, (mint, name, symbol, entry_price, entry_price, trade_size,
              ml_prob, score, narrative))
        trade_id = cur.fetchone()[0]
        conn.commit()
        cur.close()

        log.info(f"📝 PAPER TRADE ABIERTO #{trade_id} | {name or mint[:8]} | "
                 f"Entrada: ${entry_price:.8f} | Size: ${trade_size}")
        alert_paper_trade("OPEN", name, mint, entry_price)
        return trade_id
    finally:
        pool.putconn(conn)

def get_current_price(mint):
    try:
        url  = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        pairs = data.get("pairs")
        if not pairs:
            return None
        pair  = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
        price = pair.get("priceUsd")
        return float(price) if price else None
    except Exception:
        return None

def close_trade(trade_id, mint, name, entry_price, trade_size, current_price, reason):
    exit_price = current_price * (1 - FEES)
    entry      = float(entry_price)
    pnl_pct    = (exit_price - entry) / entry * 100
    pnl        = trade_size * (pnl_pct / 100)

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE paper_trades SET
                exit_price = %s, pnl = %s, pnl_pct = %s,
                status = 'CLOSED', exit_reason = %s, closed_at = NOW()
            WHERE id = %s
        """, (exit_price, pnl, pnl_pct, reason, trade_id))
        conn.commit()
        cur.close()

        log.info(f"{'✅' if pnl > 0 else '❌'} PAPER TRADE CERRADO #{trade_id} | "
                 f"{name or mint[:8]} | P&L: ${pnl:+.2f} ({pnl_pct:+.1f}%) | {reason}")
        alert_paper_trade("CLOSE", name, mint, exit_price,
                          pnl=pnl, pnl_pct=pnl_pct, reason=reason)
        return pnl, pnl_pct
    finally:
        pool.putconn(conn)

def check_new_signals():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT dt.mint, dt.name, dt.symbol, dt.price_usd,
                   dt.ml_probability, dt.survival_score, dt.narrative
            FROM discovered_tokens dt
            WHERE dt.entry_signal = 'ENTER'
              AND dt.ml_probability >= 65
              AND dt.entry_at > NOW() - INTERVAL '30 minutes'
              AND dt.mint NOT IN (
                  SELECT mint FROM paper_trades
                  WHERE status = 'OPEN'
                  OR opened_at > NOW() - INTERVAL '2 hours'
              )
            ORDER BY dt.ml_probability DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def is_daily_limit_hit():
    cap = get_capital()
    return cap['daily_pnl'] / INITIAL_CAPITAL < -DAILY_LOSS_LIMIT

def is_trading_hours():
    """Solo abrir trades en horario de mayor actividad (13h–23h UTC)"""
    hour = datetime.now(timezone.utc).hour
    return TRADE_HOURS_UTC[0] <= hour <= TRADE_HOURS_UTC[1]

def send_daily_summary():
    """Envía resumen diario por Telegram con métricas del día."""
    cap  = get_capital()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE pnl > 0)                 as wins,
                COUNT(*) FILTER (WHERE pnl <= 0)                as losses,
                COALESCE(MAX(pnl_pct), 0)                       as best_pct,
                COALESCE(MIN(pnl_pct), 0)                       as worst_pct,
                COALESCE(SUM(pnl) FILTER (WHERE closed_at::date = CURRENT_DATE - 1), 0) as day_pnl
            FROM paper_trades
            WHERE status = 'CLOSED'
        """)
        row = cur.fetchone()
        cur.close()
        wins, losses, best_pct, worst_pct, day_pnl = row
        total = (wins or 0) + (losses or 0)
        win_rate = (wins / total * 100) if total > 0 else 0
        pnl_pct  = (float(cap['daily_pnl']) / INITIAL_CAPITAL) * 100

        alert_daily_summary(
            capital   = cap['capital'],
            pnl       = cap['daily_pnl'],
            pnl_pct   = pnl_pct,
            wins      = wins or 0,
            losses    = losses or 0,
            best_trade= f"{float(best_pct):+.1f}%"
        )
        log.info(f"📊 Daily summary enviado | Capital: ${cap['capital']:.2f} | "
                 f"Win rate: {win_rate:.0f}% | P&L día: ${cap['daily_pnl']:+.2f}")
    finally:
        pool.putconn(conn)

def reset_daily_pnl():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE paper_capital SET daily_pnl = 0")
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def print_summary():
    cap = get_capital()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*),
                   COUNT(CASE WHEN pnl > 0 THEN 1 END),
                   COUNT(CASE WHEN pnl <= 0 THEN 1 END),
                   COALESCE(SUM(pnl), 0),
                   COALESCE(AVG(pnl_pct), 0),
                   COALESCE(MAX(pnl_pct), 0),
                   COALESCE(MIN(pnl_pct), 0)
            FROM paper_trades WHERE status = 'CLOSED'
        """)
        row = cur.fetchone()
        cur.close()
        total, wins, losses, total_pnl, avg_pct, best, worst = row
        win_rate = (wins / total * 100) if total > 0 else 0

        print("\n" + "="*60)
        print(f"📊 PAPER TRADING SUMMARY — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("="*60)
        print(f"  Capital inicial:  ${INITIAL_CAPITAL:.2f}")
        print(f"  Capital actual:   ${cap['capital']:.2f}")
        print(f"  P&L Total:        ${float(total_pnl):+.2f}")
        print(f"  ROI:              {(cap['capital']-INITIAL_CAPITAL)/INITIAL_CAPITAL*100:+.1f}%")
        print(f"  Total trades:     {total}")
        print(f"  Win rate:         {win_rate:.1f}% ({wins}W / {losses}L)")
        print(f"  Avg P&L:          {float(avg_pct):+.1f}%")
        print(f"  Mejor trade:      {float(best):+.1f}%")
        print(f"  Peor trade:       {float(worst):+.1f}%")
        print("="*60 + "\n")
    finally:
        pool.putconn(conn)

def run():
    setup_db()
    log.info("💰 Paper Trading Engine arrancando...")
    alert_system_status("OK", "Paper Trading Engine iniciado")

    last_daily_reset = datetime.now().date()

    while True:
        try:
            import time

            # ── RESET DIARIO ───────────────────────────────
            if datetime.now().date() > last_daily_reset:
                send_daily_summary()          # Telegram antes del reset
                reset_daily_pnl()
                last_daily_reset = datetime.now().date()

            # ── LÍMITE DIARIO ──────────────────────────────
            if is_daily_limit_hit():
                log.warning("⛔ Límite de pérdida diaria alcanzado — pausando")
                time.sleep(3600)
                continue

            cap = get_capital()

            # ── GESTIONAR TRADES ABIERTOS ──────────────────
            open_trades = get_open_trades()
            for trade in open_trades:
                tid, mint, name, symbol, entry_price, trade_size, \
                    ml_prob, score, opened_at, peak_price = trade

                current_price = get_current_price(mint)
                if not current_price:
                    continue

                entry = float(entry_price)
                peak  = float(peak_price) if peak_price else entry

                # Actualizar precio pico
                if current_price > peak:
                    peak = current_price
                    update_peak_price(tid, peak)

                # ── TAKE PROFIT ────────────────────────────
                if current_price >= entry * TAKE_PROFIT:
                    pnl, pnl_pct = close_trade(
                        tid, mint, name, entry_price,
                        float(trade_size), current_price, "TAKE_PROFIT"
                    )
                    cap['capital']   += float(trade_size) + pnl
                    cap['daily_pnl'] += pnl
                    cap['total_pnl'] += pnl
                    cap['wins' if pnl > 0 else 'losses'] += 1
                    update_capital(**cap)

                # ── TRAILING STOP ──────────────────────────
                # Activa solo si el trade subió TRAIL_ACTIVATE%+
                # Stop = peak * (1 - TRAIL_PCT)
                elif (peak >= entry * (1 + TRAIL_ACTIVATE) and
                      current_price <= peak * (1 - TRAIL_PCT)):
                    trail_level = peak * (1 - TRAIL_PCT)
                    gain_at_peak = (peak - entry) / entry * 100
                    log.info(f"🔔 TRAILING STOP #{tid} | Peak: +{gain_at_peak:.1f}% | "
                             f"Trail: ${trail_level:.8f}")
                    pnl, pnl_pct = close_trade(
                        tid, mint, name, entry_price,
                        float(trade_size), current_price, "TRAILING_STOP"
                    )
                    cap['capital']   += float(trade_size) + pnl
                    cap['daily_pnl'] += pnl
                    cap['total_pnl'] += pnl
                    cap['wins' if pnl > 0 else 'losses'] += 1
                    update_capital(**cap)

                # ── STOP LOSS FIJO ─────────────────────────
                elif current_price <= entry * STOP_LOSS:
                    pnl, pnl_pct = close_trade(
                        tid, mint, name, entry_price,
                        float(trade_size), current_price, "STOP_LOSS"
                    )
                    cap['capital']   += float(trade_size) + pnl
                    cap['daily_pnl'] += pnl
                    cap['total_pnl'] += pnl
                    cap['losses']    += 1
                    update_capital(**cap)

                # ── TIMEOUT ────────────────────────────────
                elif (datetime.now() - opened_at.replace(tzinfo=None) >
                      timedelta(minutes=MAX_HOLD_MINUTES)):
                    pnl, pnl_pct = close_trade(
                        tid, mint, name, entry_price,
                        float(trade_size), current_price, "TIMEOUT"
                    )
                    cap['capital']   += float(trade_size) + pnl
                    cap['daily_pnl'] += pnl
                    cap['total_pnl'] += pnl
                    cap['wins' if pnl > 0 else 'losses'] += 1
                    update_capital(**cap)

            # ── ABRIR NUEVOS TRADES ────────────────────────
            # Solo en horario activo (13h–23h UTC)
            if is_trading_hours():
                open_trades = get_open_trades()
                if len(open_trades) < MAX_OPEN_TRADES:
                    signals = check_new_signals()
                    for sig in signals:
                        if len(get_open_trades()) >= MAX_OPEN_TRADES:
                            break
                        mint, name, symbol, price, ml_prob, score, narrative = sig
                        if price:
                            cap = get_capital()
                            open_trade(mint, name, symbol, price,
                                       ml_prob, score, narrative, cap['capital'])
                            cap['capital'] -= cap['capital'] * TRADE_SIZE_PCT
                            update_capital(**cap)
            else:
                hour = datetime.now(timezone.utc).hour
                log.debug(f"🌙 Fuera de horario ({hour}h UTC) — no se abren nuevos trades")

            print_summary()
            time.sleep(60)

        except Exception as e:
            log.error(f"Error en paper trading: {e}")
            import time
            time.sleep(30)

if __name__ == "__main__":
    run()
