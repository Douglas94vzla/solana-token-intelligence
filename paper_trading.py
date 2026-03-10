import psycopg2
import psycopg2.pool
import os
import logging
import requests
from datetime import datetime, timedelta
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
INITIAL_CAPITAL     = 1000.0   # Capital simulado
TRADE_SIZE_PCT      = 0.02     # 2% por trade (Kelly conservador)
MAX_OPEN_TRADES     = 3        # Máximo trades simultáneos
TAKE_PROFIT         = 1.50     # +50% TP
STOP_LOSS           = 0.70     # -30% SL
MAX_HOLD_MINUTES    = 120      # Máximo 2 horas por trade
DAILY_LOSS_LIMIT    = 0.03     # Parar si perdemos 3% en el día
SLIPPAGE            = 0.03     # 3% slippage simulado
FEES                = 0.005    # 0.5% fees

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
                   ml_probability, survival_score, opened_at
            FROM paper_trades
            WHERE status = 'OPEN'
            ORDER BY opened_at ASC
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def open_trade(mint, name, symbol, price, ml_prob, score, narrative, capital):
    """Abre un nuevo paper trade"""
    trade_size = round(capital * TRADE_SIZE_PCT, 2)
    entry_price = float(price) * (1 + SLIPPAGE + FEES)  # Simular slippage

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO paper_trades
            (mint, name, symbol, entry_price, trade_size, ml_probability,
             survival_score, narrative, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'OPEN')
            RETURNING id
        """, (mint, name, symbol, entry_price, trade_size,
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
    """Obtiene precio actual de DexScreener"""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=10)
        data = resp.json()
        pairs = data.get("pairs")
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
        price = pair.get("priceUsd")
        return float(price) if price else None
    except Exception:
        return None

def close_trade(trade_id, mint, name, entry_price, trade_size, current_price, reason):
    """Cierra un paper trade"""
    exit_price = current_price * (1 - FEES)  # Simular fees de salida
    entry       = float(entry_price)
    pnl_pct     = (exit_price - entry) / entry * 100
    pnl         = trade_size * (pnl_pct / 100)

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
    """Busca señales ENTER nuevas para abrir trades"""
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
    """Verifica si alcanzamos el límite de pérdida diaria"""
    cap = get_capital()
    daily_loss_pct = cap['daily_pnl'] / INITIAL_CAPITAL
    return daily_loss_pct < -DAILY_LOSS_LIMIT

def reset_daily_pnl():
    """Resetea el P&L diario a medianoche"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE paper_capital SET daily_pnl = 0")
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def print_summary():
    """Imprime resumen del paper trading"""
    cap = get_capital()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN pnl > 0 THEN 1 END) as wins,
                   COUNT(CASE WHEN pnl <= 0 THEN 1 END) as losses,
                   COALESCE(SUM(pnl), 0) as total_pnl,
                   COALESCE(AVG(pnl_pct), 0) as avg_pnl_pct,
                   COALESCE(MAX(pnl_pct), 0) as best_trade,
                   COALESCE(MIN(pnl_pct), 0) as worst_trade
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

            # Reset diario
            if datetime.now().date() > last_daily_reset:
                reset_daily_pnl()
                last_daily_reset = datetime.now().date()
                cap = get_capital()
                send_message(
                    f"🌅 <b>Nuevo día de trading</b>\n"
                    f"Capital: ${cap['capital']:.2f} | "
                    f"P&L total: ${cap['total_pnl']:+.2f}"
                )

            # Verificar límite diario
            if is_daily_limit_hit():
                log.warning("⛔ Límite de pérdida diaria alcanzado — pausando")
                time.sleep(3600)
                continue

            cap = get_capital()

            # ── GESTIONAR TRADES ABIERTOS ─────────────────
            open_trades = get_open_trades()
            for trade in open_trades:
                tid, mint, name, symbol, entry_price, trade_size, ml_prob, score, opened_at = trade

                current_price = get_current_price(mint)
                if not current_price:
                    continue

                entry   = float(entry_price)
                change  = (current_price - entry) / entry

                # Take profit
                if current_price >= entry * TAKE_PROFIT:
                    pnl, pnl_pct = close_trade(
                        tid, mint, name, entry_price,
                        float(trade_size), current_price, "TAKE_PROFIT"
                    )
                    cap['capital']   += float(trade_size) + pnl
                    cap['daily_pnl'] += pnl
                    cap['total_pnl'] += pnl
                    if pnl > 0:
                        cap['wins'] += 1
                    else:
                        cap['losses'] += 1
                    update_capital(**cap)

                # Stop loss
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

                # Timeout
                elif datetime.now() - opened_at.replace(tzinfo=None) > timedelta(minutes=MAX_HOLD_MINUTES):
                    pnl, pnl_pct = close_trade(
                        tid, mint, name, entry_price,
                        float(trade_size), current_price, "TIMEOUT"
                    )
                    cap['capital']   += float(trade_size) + pnl
                    cap['daily_pnl'] += pnl
                    cap['total_pnl'] += pnl
                    if pnl > 0:
                        cap['wins'] += 1
                    else:
                        cap['losses'] += 1
                    update_capital(**cap)

            # ── ABRIR NUEVOS TRADES ───────────────────────
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

            print_summary()
            time.sleep(60)

        except Exception as e:
            log.error(f"Error en paper trading: {e}")
            import time
            time.sleep(30)

if __name__ == "__main__":
    run()
