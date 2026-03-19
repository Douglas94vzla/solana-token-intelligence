import psycopg2
import psycopg2.pool
import os
import time
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
INITIAL_CAPITAL       = 1000.0   # Capital simulado
TRADE_SIZE_PCT        = 0.02     # 2% por trade
MAX_OPEN_TRADES       = 3        # Máximo trades simultáneos
TAKE_PROFIT           = 1.50     # +50% TP
STOP_LOSS             = 0.70     # -30% stop inicial fijo
TRAIL_PCT             = 0.20     # Trailing: cierra si cae 20% desde el pico
TRAIL_ACTIVATE        = 0.10     # Trailing activa solo si el trade subió 10%+
MAX_HOLD_MINUTES      = 120      # Timeout 2 horas
DAILY_LOSS_LIMIT      = 0.03     # Parar si perdemos 3% en el día
SLIPPAGE              = 0.03     # Slippage base (fallback si no hay liquidez)
FEES                  = 0.005    # 0.5% fees
TRADE_HOURS_UTC       = (13, 23) # Solo abrir trades entre 13h y 23h UTC
POSITION_CHECK_INTERVAL = 10     # Revisar posiciones abiertas cada 10s
SIGNAL_CHECK_INTERVAL   = 60     # Buscar señales nuevas cada 60s
STOP_LOSS_MIN         = 0.85     # Stop adaptativo mínimo: -15%
STOP_LOSS_MAX         = 0.70     # Stop adaptativo máximo: -30%
TRAILING_REENTRY_COOLDOWN = 15   # Minutos de espera antes de re-entrada tras trailing stop

# ── MULTI-STRATEGY CONFIG (mejora 14) ─────────────────
# Cada estrategia corre en paralelo con su propio capital virtual.
# STANDARD = cuenta principal (paper_capital). Las otras son simulaciones.
STRATEGIES = {
    'CONSERVATIVE': {
        'ml_min':         50,       # ML calibrado ≥50% (2x base rate ~23%)
        'max_open':        2,       # Máximo 2 trades simultáneos
        'size_pct':       0.01,     # 1% fijo por trade (preservar capital)
        'initial_capital': 333.0,   # Capital inicial de la estrategia
    },
    'STANDARD': {
        'ml_min':         35,       # ML calibrado ≥35% (1.5x base rate)
        'max_open':        3,       # Máximo 3 trades (existente)
        'size_pct':       None,     # Usa Kelly sizing
        'initial_capital': 333.0,   # Capital inicial de la estrategia
    },
    'AGGRESSIVE': {
        'ml_min':         35,       # Mismo filtro ML que STANDARD
        'max_open':        3,       # Máximo 3 trades
        'size_pct':       0.05,     # 5% fijo (apuesta mayor en cada trade)
        'initial_capital': 333.0,   # Capital inicial de la estrategia
    },
}

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
        # Tabla de capital por estrategia (mejora 14)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS strategy_capital (
                strategy         TEXT PRIMARY KEY,
                capital          NUMERIC(20,2) DEFAULT 333.0,
                initial_capital  NUMERIC(20,2) DEFAULT 333.0,
                wins             INTEGER DEFAULT 0,
                losses           INTEGER DEFAULT 0,
                total_pnl        NUMERIC(20,2) DEFAULT 0,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Poblar estrategias si no existen
        for strat, cfg in STRATEGIES.items():
            cur.execute("""
                INSERT INTO strategy_capital (strategy, capital, initial_capital)
                VALUES (%s, %s, %s) ON CONFLICT (strategy) DO NOTHING
            """, (strat, cfg['initial_capital'], cfg['initial_capital']))
        conn.commit()
        # Migraciones de columnas
        try:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS peak_price NUMERIC(20,10)")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS stop_loss_pct NUMERIC(6,4)")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS strategy TEXT DEFAULT 'STANDARD'")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS slippage_pct NUMERIC(6,4)")
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

def get_open_trades(strategy=None):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        if strategy:
            cur.execute("""
                SELECT id, mint, name, symbol, entry_price, trade_size,
                       ml_probability, survival_score, opened_at,
                       COALESCE(peak_price, entry_price) as peak_price,
                       COALESCE(stop_loss_pct, %s) as stop_loss_pct,
                       COALESCE(strategy, 'STANDARD') as strategy
                FROM paper_trades
                WHERE status = 'OPEN' AND COALESCE(strategy, 'STANDARD') = %s
                ORDER BY opened_at ASC
            """, (STOP_LOSS, strategy))
        else:
            cur.execute("""
                SELECT id, mint, name, symbol, entry_price, trade_size,
                       ml_probability, survival_score, opened_at,
                       COALESCE(peak_price, entry_price) as peak_price,
                       COALESCE(stop_loss_pct, %s) as stop_loss_pct,
                       COALESCE(strategy, 'STANDARD') as strategy
                FROM paper_trades
                WHERE status = 'OPEN'
                ORDER BY opened_at ASC
            """, (STOP_LOSS,))
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

def kelly_size(ml_prob, capital):
    """
    Half-Kelly position sizing basado en probabilidad ML.
    b = ratio ganancia/pérdida esperado (TAKE_PROFIT gain / STOP_LOSS loss).
    Retorna (trade_size_usd, pct_aplicado).
    Clampado entre 1% y 5% del capital.
    """
    p = (ml_prob or 65) / 100.0
    b = (TAKE_PROFIT - 1) / (1 - STOP_LOSS)   # 0.50 / 0.30 ≈ 1.667
    kelly_f    = (p * b - (1 - p)) / b
    half_kelly = max(0.0, kelly_f / 2.0)
    pct        = max(0.01, min(0.05, half_kelly))
    return round(capital * pct, 2), round(pct * 100, 1)

def dynamic_slippage(trade_size, liquidity_usd):
    """
    Mejora 13: Slippage variable basado en liquidez real de DexScreener.
    trade_size / liquidity_usd = impacto de mercado estimado.
    Ej: $20 trade en pool de $5k → slippage = 20/5000 = 0.4% (mucho mejor que 3% fijo)
    Ej: $20 trade en pool de $200 → slippage = 10% (realista para tokens micro-cap)
    Clampado entre 0.5% y 30%.
    """
    if not liquidity_usd or liquidity_usd <= 0:
        return SLIPPAGE   # fallback al 3% base
    slip = trade_size / max(float(liquidity_usd), 1.0)
    return round(min(0.30, max(0.005, slip)), 4)

def get_strategy_capital(strategy):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT capital, initial_capital, wins, losses, total_pnl
            FROM strategy_capital WHERE strategy = %s
        """, (strategy,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {
            'capital':         float(row[0]),
            'initial_capital': float(row[1]),
            'wins':            row[2],
            'losses':          row[3],
            'total_pnl':       float(row[4]),
        }
    finally:
        pool.putconn(conn)

def update_strategy_capital(strategy, capital, wins, losses, total_pnl):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE strategy_capital SET
                capital = %s, wins = %s, losses = %s,
                total_pnl = %s, updated_at = NOW()
            WHERE strategy = %s
        """, (capital, wins, losses, total_pnl, strategy))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def calc_adaptive_stop(mint):
    """
    Stop-loss dinámico basado en ATR de los últimos snapshots.
    Stop distance = 2.5x ATR, clamped entre -15% y -30%.
    Retorna multiplicador: ej. 0.80 → stop en -20%.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT price_usd FROM price_snapshots
            WHERE mint = %s AND price_usd > 0
            ORDER BY snapshot_at DESC LIMIT 10
        """, (mint,))
        prices = [float(r[0]) for r in cur.fetchall()]
        cur.close()
    finally:
        pool.putconn(conn)

    if len(prices) < 3:
        return STOP_LOSS  # fallback al default -30%

    changes  = [abs(prices[i] - prices[i+1]) / prices[i+1]
                for i in range(len(prices) - 1)]
    atr      = sum(changes) / len(changes)
    stop_dist = min(1 - STOP_LOSS_MAX, max(1 - STOP_LOSS_MIN, atr * 2.5))
    return round(1.0 - stop_dist, 4)

def open_trade(mint, name, symbol, price, ml_prob, score, narrative, trade_size,
               strategy='STANDARD', liquidity_usd=None):
    slip        = dynamic_slippage(trade_size, liquidity_usd)   # mejora 13
    entry_price = float(price) * (1 + slip + FEES)
    stop_pct    = calc_adaptive_stop(mint)

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO paper_trades
            (mint, name, symbol, entry_price, peak_price, trade_size,
             ml_probability, survival_score, narrative, stop_loss_pct,
             strategy, slippage_pct, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'OPEN')
            RETURNING id
        """, (mint, name, symbol, entry_price, entry_price, trade_size,
              ml_prob, score, narrative, stop_pct, strategy, slip))
        trade_id = cur.fetchone()[0]
        conn.commit()
        cur.close()

        stop_pct_display = round((1 - stop_pct) * 100, 1)
        slip_pct_display = round(slip * 100, 2)
        log.info(f"📝 [{strategy}] TRADE ABIERTO #{trade_id} | {name or mint[:8]} | "
                 f"Entrada: ${entry_price:.8f} | Size: ${trade_size} | "
                 f"Slippage: {slip_pct_display}% | Stop: -{stop_pct_display}% | ML: {ml_prob}%")
        if strategy == 'STANDARD':
            alert_paper_trade("OPEN", name, mint, entry_price)
        return trade_id
    finally:
        pool.putconn(conn)

def get_current_price(mint):
    # Fuente 1: DexScreener
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

    # Fuente 2 (fallback): Jupiter Price API
    try:
        url  = f"https://api.jup.ag/price/v2?ids={mint}"
        resp = requests.get(url, timeout=8)
        data = resp.json()
        price_data = data.get("data", {}).get(mint)
        if price_data and price_data.get("price"):
            log.debug(f"Jupiter fallback usado para {mint[:8]}")
            return float(price_data["price"])
    except Exception:
        pass

    return None

def close_trade(trade_id, mint, name, entry_price, trade_size, current_price, reason,
                strategy='STANDARD'):
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

        emoji = '✅' if pnl > 0 else '❌'
        log.info(f"{emoji} [{strategy}] TRADE CERRADO #{trade_id} | "
                 f"{name or mint[:8]} | P&L: ${pnl:+.2f} ({pnl_pct:+.1f}%) | {reason}")
        if strategy == 'STANDARD':
            alert_paper_trade("CLOSE", name, mint, exit_price,
                              pnl=pnl, pnl_pct=pnl_pct, reason=reason)
        return pnl, pnl_pct
    finally:
        pool.putconn(conn)

def check_new_signals(ml_min=65):
    """Retorna señales ENTER con ml_probability >= ml_min, incluyendo liquidity_usd."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT dt.mint, dt.name, dt.symbol, dt.price_usd,
                   dt.ml_probability, dt.survival_score, dt.narrative,
                   dt.liquidity_usd
            FROM discovered_tokens dt
            WHERE dt.entry_signal = 'ENTER'
              AND dt.ml_probability >= %s
              AND dt.entry_at > NOW() - INTERVAL '30 minutes'
              AND dt.market_cap >= 2000
              AND (
                dt.pair_address IS NULL                    -- bonding curve: sin par DexScreener
                OR dt.liquidity_usd >= 3000               -- con par: liquidez mínima
              )
              AND dt.mint NOT IN (
                  SELECT mint FROM paper_trades
                  WHERE status = 'OPEN'
                     OR (exit_reason IN ('STOP_LOSS', 'TAKE_PROFIT', 'TIMEOUT')
                         AND opened_at > NOW() - INTERVAL '2 hours')
                     OR (exit_reason = 'TRAILING_STOP'
                         AND closed_at > NOW() - INTERVAL '15 minutes')
              )
            ORDER BY dt.ml_probability DESC
            LIMIT 5
        """, (ml_min,))
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
              AND COALESCE(strategy, 'STANDARD') = 'STANDARD'
        """)
        row = cur.fetchone()
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
        print("="*60)

        # ── MULTI-STRATEGY COMPARISON (mejora 14) ─────────
        print("\n  RENDIMIENTO POR ESTRATEGIA:")
        print(f"  {'Estrategia':<14} {'Capital':>10} {'ROI':>8} {'Trades':>7} {'WR':>6}")
        print(f"  {'-'*48}")
        for strat_name, scfg in STRATEGIES.items():
            if strat_name == 'STANDARD':
                s_cap  = cap['capital']
                s_init = INITIAL_CAPITAL
            else:
                scap   = get_strategy_capital(strat_name)
                s_cap  = scap['capital'] if scap else scfg['initial_capital']
                s_init = scfg['initial_capital']
            cur.execute("""
                SELECT COUNT(*), COUNT(CASE WHEN pnl > 0 THEN 1 END)
                FROM paper_trades WHERE status = 'CLOSED'
                  AND COALESCE(strategy, 'STANDARD') = %s
            """, (strat_name,))
            sr = cur.fetchone()
            s_trades = sr[0] if sr else 0
            s_wins   = sr[1] if sr else 0
            s_wr     = (s_wins / s_trades * 100) if s_trades > 0 else 0
            s_roi    = (s_cap - s_init) / s_init * 100
            print(f"  {strat_name:<14} ${s_cap:>8.2f} {s_roi:>+7.1f}% {s_trades:>6} {s_wr:>5.0f}%")
        print("="*60 + "\n")
        cur.close()
    finally:
        pool.putconn(conn)

def _apply_pnl_to_cap(cap, strategy, pnl, trade_size, is_win):
    """Actualiza el capital correcto según la estrategia."""
    if strategy == 'STANDARD':
        cap['capital']   += float(trade_size) + pnl
        cap['daily_pnl'] += pnl
        cap['total_pnl'] += pnl
        cap['wins' if is_win else 'losses'] += 1
        update_capital(**cap)
    else:
        # Actualizar capital de la estrategia alternativa
        scap = get_strategy_capital(strategy)
        if scap:
            scap['capital']   += float(trade_size) + pnl
            scap['total_pnl'] += pnl
            scap['wins' if is_win else 'losses'] += 1
            update_strategy_capital(strategy, scap['capital'],
                                    scap['wins'], scap['losses'], scap['total_pnl'])

# Contador de ciclos sin precio por trade_id (en memoria)
_no_price_count: dict[int, int] = {}
# Máx ciclos sin precio antes de cerrar al stop máximo (6 × 10s = 60s)
NO_PRICE_LIMIT = 6


def manage_open_trades(cap):
    """Revisa y gestiona todas las posiciones abiertas. Llamado cada 10s."""
    open_trades = get_open_trades()
    for trade in open_trades:
        tid, mint, name, symbol, entry_price, trade_size, \
            ml_prob, score, opened_at, peak_price, stop_loss_pct, strategy = trade

        current_price = get_current_price(mint)
        if not current_price:
            _no_price_count[tid] = _no_price_count.get(tid, 0) + 1
            if _no_price_count[tid] >= NO_PRICE_LIMIT:
                # Sin precio por 60s+: cerrar al precio de stop máximo
                forced_price = float(entry_price) * STOP_LOSS
                log.warning(f"⚠️  SIN PRECIO #{tid} | {name or mint[:8]} | "
                            f"{_no_price_count[tid]} ciclos sin datos — cerrando al stop máximo")
                pnl, pnl_pct = close_trade(
                    tid, mint, name, entry_price,
                    float(trade_size), forced_price, "STOP_LOSS", strategy
                )
                _apply_pnl_to_cap(cap, strategy, pnl, trade_size, False)
                _no_price_count.pop(tid, None)
            continue
        _no_price_count.pop(tid, None)  # reset al recuperar precio

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
                float(trade_size), current_price, "TAKE_PROFIT", strategy
            )
            _apply_pnl_to_cap(cap, strategy, pnl, trade_size, pnl > 0)

        # ── TRAILING STOP ──────────────────────────
        # Activa solo si el trade subió TRAIL_ACTIVATE%+
        # Stop = peak * (1 - TRAIL_PCT)
        elif (peak >= entry * (1 + TRAIL_ACTIVATE) and
              current_price <= peak * (1 - TRAIL_PCT)):
            trail_level  = peak * (1 - TRAIL_PCT)
            gain_at_peak = (peak - entry) / entry * 100
            log.info(f"🔔 TRAILING STOP #{tid} [{strategy}] | Peak: +{gain_at_peak:.1f}% | "
                     f"Trail: ${trail_level:.8f}")
            pnl, pnl_pct = close_trade(
                tid, mint, name, entry_price,
                float(trade_size), current_price, "TRAILING_STOP", strategy
            )
            _apply_pnl_to_cap(cap, strategy, pnl, trade_size, pnl > 0)

        # ── STOP LOSS ADAPTATIVO ───────────────────
        elif current_price <= entry * float(stop_loss_pct):
            pnl, pnl_pct = close_trade(
                tid, mint, name, entry_price,
                float(trade_size), current_price, "STOP_LOSS", strategy
            )
            _apply_pnl_to_cap(cap, strategy, pnl, trade_size, False)

        # ── TIMEOUT ────────────────────────────────
        elif (datetime.now() - opened_at.replace(tzinfo=None) >
              timedelta(minutes=MAX_HOLD_MINUTES)):
            pnl, pnl_pct = close_trade(
                tid, mint, name, entry_price,
                float(trade_size), current_price, "TIMEOUT", strategy
            )
            _apply_pnl_to_cap(cap, strategy, pnl, trade_size, pnl > 0)

    return cap


def run():
    setup_db()
    log.info("💰 Paper Trading Engine arrancando...")
    log.info(f"⏱  Ciclo posiciones: {POSITION_CHECK_INTERVAL}s | Ciclo señales: {SIGNAL_CHECK_INTERVAL}s")
    alert_system_status("OK", "Paper Trading Engine iniciado")

    last_daily_reset  = datetime.now().date()
    last_signal_check = 0.0   # timestamp de la última búsqueda de señales
    last_summary      = 0.0

    while True:
        try:
            now = time.monotonic()

            # ── RESET DIARIO ───────────────────────────────
            if datetime.now().date() > last_daily_reset:
                send_daily_summary()
                reset_daily_pnl()
                last_daily_reset = datetime.now().date()

            cap = get_capital()

            # ── GESTIONAR POSICIONES ABIERTAS (cada 10s) ───
            # SIEMPRE gestionar posiciones abiertas, incluso con límite diario activo
            cap = manage_open_trades(cap)

            # ── LÍMITE DIARIO — bloquea solo apertura de nuevos trades ──
            if is_daily_limit_hit():
                log.warning("⛔ Límite de pérdida diaria alcanzado — no se abren nuevos trades")
                time.sleep(POSITION_CHECK_INTERVAL)
                continue

            # ── ABRIR NUEVOS TRADES (cada 60s) ─────────────
            if now - last_signal_check >= SIGNAL_CHECK_INTERVAL:
                last_signal_check = now
                if is_trading_hours():
                    # Señales ordenadas por ML prob (ml_min=65 es el filtro más amplio)
                    signals = check_new_signals(ml_min=35)
                    for sig in signals:
                        mint, name, symbol, price, ml_prob, score, narrative, liquidity = sig
                        if not price:
                            continue
                        # ── Evaluar cada estrategia para este token ───────
                        for strat_name, scfg in STRATEGIES.items():
                            if (ml_prob or 0) < scfg['ml_min']:
                                continue
                            open_count = len(get_open_trades(strategy=strat_name))
                            if open_count >= scfg['max_open']:
                                continue
                            # Calcular tamaño según estrategia
                            if strat_name == 'STANDARD':
                                cap = get_capital()
                                t_size, size_pct = kelly_size(ml_prob or 65, cap['capital'])
                                log.info(f"💰 [{strat_name}] Kelly: {size_pct:.1f}% = ${t_size:.2f} | ML={ml_prob}%")
                            else:
                                scap = get_strategy_capital(strat_name)
                                if not scap:
                                    continue
                                t_size = round(scap['capital'] * scfg['size_pct'], 2)
                                log.info(f"💰 [{strat_name}] Fixed: {scfg['size_pct']*100:.0f}% = ${t_size:.2f} | ML={ml_prob}%")
                            open_trade(mint, name, symbol, price,
                                       ml_prob, score, narrative, t_size,
                                       strategy=strat_name, liquidity_usd=liquidity)
                            # Descontar capital de la estrategia correspondiente
                            if strat_name == 'STANDARD':
                                cap = get_capital()
                                cap['capital'] -= t_size
                                update_capital(**cap)
                            else:
                                scap = get_strategy_capital(strat_name)
                                if scap:
                                    scap['capital'] -= t_size
                                    update_strategy_capital(strat_name, scap['capital'],
                                                            scap['wins'], scap['losses'], scap['total_pnl'])
                else:
                    hour = datetime.now(timezone.utc).hour
                    log.debug(f"🌙 Fuera de horario ({hour}h UTC) — no se abren nuevos trades")

                if now - last_summary >= SIGNAL_CHECK_INTERVAL:
                    last_summary = now
                    print_summary()

            time.sleep(POSITION_CHECK_INTERVAL)

        except Exception as e:
            log.error(f"Error en paper trading: {e}")
            time.sleep(30)

if __name__ == "__main__":
    run()
