import psycopg2
import psycopg2.pool
import os
import time
import logging
import math
import statistics
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from telegram_bot import (alert_paper_trade, alert_daily_summary,
                           alert_system_status, send_message,
                           alert_consecutive_losses, alert_no_ml_samples)

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
TAKE_PROFIT           = 1.80     # +80% TP final (50% restante) — subido de 1.50 el 2026-04-10
PARTIAL_TP_PCT        = 1.30     # +30% → cierra 50% de la posición — subido de 1.20 el 2026-04-10
STOP_LOSS             = 0.70     # -30% stop inicial fijo
TRAIL_PCT             = 0.20     # Trailing: cierra si cae 20% desde el pico
TRAIL_ACTIVATE        = 0.10     # Trailing activa solo si el trade subió 10%+
MAX_HOLD_MINUTES      = 30       # Timeout 30 minutos (tokens que no despegan en 30min ya no despegan)
DAILY_LOSS_LIMIT      = 0.03     # Parar si perdemos 3% en el día
SLIPPAGE              = 0.03     # Slippage base (fallback si no hay liquidez)
FEES                  = 0.005    # 0.5% fees
TRADE_HOURS_UTC_WINDOWS = [(13, 18), (22, 24)]  # 13–17h y 22–23h UTC (evita 18–21h: WR 35–41%)
POSITION_CHECK_INTERVAL = 10     # Revisar posiciones abiertas cada 10s
SIGNAL_CHECK_INTERVAL   = 60     # Buscar señales nuevas cada 60s
STOP_LOSS_MIN         = 0.85     # Stop adaptativo mínimo: -15%
STOP_LOSS_MAX         = 0.70     # Stop adaptativo máximo: -30%
TRAILING_REENTRY_COOLDOWN = 15   # Minutos de espera antes de re-entrada tras trailing stop
BLOCKED_NARRATIVES = ('AI/AGI', 'IDENTITY', 'NUMBERS')  # WR < 40% histórico — destruyen capital
EARLY_TOKEN_MAX_MINUTES   = 15   # EARLY_ENTRY: máxima edad del token en minutos
EMERGENCY_EXIT_PCT        = 0.50 # Emergency exit: salir si precio cae >50% desde entry
EMERGENCY_EXIT_MINUTES    = 5    # Solo aplica si el trade tiene menos de 5 min de vida
EXTENDED_TP_BOOST         = 0.20 # Extensión de TP si momentum fuerte: TAKE_PROFIT + 20%

# ── VOLATILITY TARGETING (paso 6) ─────────────────────
VOL_TARGET = 0.25   # 25% volatilidad objetivo (decimal)
VOL_WINDOW = 20     # últimos N trades cerrados para medir vol

# ── CROSS-ASSET FILTER (paso 8) ────────────────────────
SOL_DROP_THRESHOLD = -5.0   # pausar si SOL cae >5% en 1h
BTC_DROP_THRESHOLD = -3.0   # pausar si BTC cae >3% en 1h
MACRO_CACHE_TTL    = 300    # refrescar precios macro cada 5 min

# ── MULTI-STRATEGY CONFIG (mejora 14) ─────────────────
# Cada estrategia corre en paralelo con su propio capital virtual.
# STANDARD = cuenta principal (paper_capital). Las otras son simulaciones.
# signal_source: 'standard' → usa check_new_signals() (requiere entry_signal=ENTER)
#                'early'    → usa check_early_entry_signals() (sin filtros de social/liquidez)
STRATEGIES = {
    'CONSERVATIVE': {
        'ml_min':          80,   # subido de 50 el 2026-04-10 — análisis 106 trades
        'max_open':         2,
        'size_pct':        0.01,
        'initial_capital': 333.0,
        'signal_source':   'standard',
    },
    'STANDARD': {
        'ml_min':          80,   # subido de 35 el 2026-04-10 — ML 65-79% WR 50% PnL negativo
        'max_open':         3,
        'size_pct':        None,    # Usa Kelly sizing
        'initial_capital': 333.0,
        'signal_source':   'standard',
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
        # Tabla de missed trades (compartida con entry_signal.py — idempotente)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS missed_trades (
                id               SERIAL PRIMARY KEY,
                mint             TEXT NOT NULL,
                name             TEXT,
                symbol           TEXT,
                ml_probability   NUMERIC(5,2),
                rejection_stage  TEXT,
                rejection_reason TEXT,
                rejection_detail TEXT,
                strategy         TEXT,
                entry_price      NUMERIC(20,10),
                missed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                price_30m        NUMERIC(20,10),
                price_1h         NUMERIC(20,10),
                price_2h         NUMERIC(20,10),
                pnl_pct_30m      NUMERIC(10,2),
                pnl_pct_1h       NUMERIC(10,2),
                pnl_pct_2h       NUMERIC(10,2),
                phantom_pnl_30m  NUMERIC(10,2),
                phantom_pnl_1h   NUMERIC(10,2),
                phantom_pnl_2h   NUMERIC(10,2),
                tracked_30m      BOOLEAN DEFAULT FALSE,
                tracked_1h       BOOLEAN DEFAULT FALSE,
                tracked_2h       BOOLEAN DEFAULT FALSE
            );
            CREATE INDEX IF NOT EXISTS idx_missed_trades_mint ON missed_trades(mint);
            CREATE INDEX IF NOT EXISTS idx_missed_trades_at   ON missed_trades(missed_at DESC);
        """)
        conn.commit()
        try:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS peak_price NUMERIC(20,10)")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS stop_loss_pct NUMERIC(6,4)")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS strategy TEXT DEFAULT 'STANDARD'")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS slippage_pct NUMERIC(6,4)")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS partial_tp_taken BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE strategy_capital ADD COLUMN IF NOT EXISTS peak_capital NUMERIC(20,2)")
            cur.execute("""
                UPDATE strategy_capital SET peak_capital = GREATEST(initial_capital, capital)
                WHERE peak_capital IS NULL
            """)
            conn.commit()
        except Exception:
            conn.rollback()
        cur.close()
        log.info("✅ Tablas paper trading creadas")
    finally:
        pool.putconn(conn)

def log_missed_trade(mint, name, symbol, ml_prob, stage, reason, detail, price, strategy=None):
    """Registra un trade rechazado para análisis post-mortem. Nunca lanza excepción."""
    try:
        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO missed_trades
                    (mint, name, symbol, ml_probability, rejection_stage,
                     rejection_reason, rejection_detail, strategy, entry_price)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM missed_trades
                    WHERE mint = %s AND rejection_reason = %s
                      AND (strategy = %s OR (%s IS NULL AND strategy IS NULL))
                      AND missed_at > NOW() - INTERVAL '2 hours'
                )
            """, (mint, name, symbol, ml_prob, stage, reason, detail, strategy, price,
                  mint, reason, strategy, strategy))
            conn.commit()
            cur.close()
        finally:
            pool.putconn(conn)
    except Exception as e:
        log.warning(f"log_missed_trade error: {e}")

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
                       COALESCE(strategy, 'STANDARD') as strategy,
                       COALESCE(partial_tp_taken, FALSE) as partial_tp_taken
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
                       COALESCE(strategy, 'STANDARD') as strategy,
                       COALESCE(partial_tp_taken, FALSE) as partial_tp_taken
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
    p = float(ml_prob or 65) / 100.0
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
            SELECT capital, initial_capital, wins, losses, total_pnl,
                   COALESCE(peak_capital, initial_capital) as peak_capital
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
            'peak_capital':    float(row[5]),
        }
    finally:
        pool.putconn(conn)

def update_strategy_capital(strategy, capital, wins, losses, total_pnl):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE strategy_capital SET
                capital      = %s,
                wins         = %s,
                losses       = %s,
                total_pnl    = %s,
                updated_at   = NOW(),
                peak_capital = GREATEST(COALESCE(peak_capital, initial_capital), %s)
            WHERE strategy = %s
        """, (capital, wins, losses, total_pnl, capital, strategy))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

# Throttle: una alerta por estrategia cada hora para drawdown >15%
_dd_alerted: dict[str, float] = {}

def get_drawdown_factor(strategy: str) -> float:
    """
    Factor de sizing basado en drawdown desde el pico histórico de capital.
    < 5%  → 1.00 (sin cambio)
    5-10% → 0.75
    10-15%→ 0.50
    > 15% → 0.00 (pausa + alerta Telegram, máx 1 alerta/hora)
    """
    scap = get_strategy_capital(strategy)
    if not scap:
        return 1.0
    peak    = scap['peak_capital']
    current = scap['capital']
    if peak <= 0:
        return 1.0
    drawdown = (peak - current) / peak
    if drawdown < 0.05:
        return 1.0
    if drawdown < 0.10:
        log.info(f"📉 [{strategy}] Drawdown {drawdown*100:.1f}% — sizing ×0.75")
        return 0.75
    if drawdown < 0.15:
        log.warning(f"📉 [{strategy}] Drawdown {drawdown*100:.1f}% — sizing ×0.50")
        return 0.50
    # > 15%: pausa total
    now_ts = time.monotonic()
    if now_ts - _dd_alerted.get(strategy, 0) > 3600:
        _dd_alerted[strategy] = now_ts
        send_message(
            f"🚨 DRAWDOWN CRÍTICO [{strategy}] | "
            f"{drawdown*100:.1f}% desde pico ${peak:.2f} → ${current:.2f} | "
            f"⛔ Pausando nuevas entradas"
        )
        log.error(f"🚨 [{strategy}] Drawdown {drawdown*100:.1f}% > 15% — PAUSANDO")
    return 0.0


_macro_cache: dict = {'sol': None, 'btc': None, 'fetched_at': 0.0}
_macro_alerted_at: float = 0.0

def get_macro_risk() -> tuple:
    """
    Consulta el cambio de precio de SOL y BTC en la última hora vía CoinGecko.
    Resultado cacheado MACRO_CACHE_TTL segundos para no saturar la API.
    Retorna (is_risk_off: bool, motivo: str).
    Falla silenciosa si la API no responde — nunca bloquea por error externo.
    """
    global _macro_cache, _macro_alerted_at
    now_ts = time.monotonic()

    if now_ts - _macro_cache['fetched_at'] >= MACRO_CACHE_TTL:
        try:
            url  = ("https://api.coingecko.com/api/v3/coins/markets"
                    "?vs_currency=usd&ids=solana,bitcoin&price_change_percentage=1h")
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            sol_ch = btc_ch = None
            for coin in resp.json():
                ch = coin.get('price_change_percentage_1h_in_currency')
                if coin['id'] == 'solana':
                    sol_ch = ch
                elif coin['id'] == 'bitcoin':
                    btc_ch = ch
            _macro_cache = {'sol': sol_ch, 'btc': btc_ch, 'fetched_at': now_ts}
            log.debug(f"🌍 Macro: SOL {sol_ch:+.2f}% | BTC {btc_ch:+.2f}% (1h)")
        except Exception as e:
            log.debug(f"get_macro_risk error: {e}")
            return False, ""   # falla silenciosa → no bloquear por API caída

    sol_ch = _macro_cache['sol']
    btc_ch = _macro_cache['btc']

    if sol_ch is not None and sol_ch < SOL_DROP_THRESHOLD:
        motivo = f"SOL {sol_ch:+.1f}% en 1h"
    elif btc_ch is not None and btc_ch < BTC_DROP_THRESHOLD:
        motivo = f"BTC {btc_ch:+.1f}% en 1h"
    else:
        return False, ""

    # Alerta Telegram throttled a 1 vez/hora
    if now_ts - _macro_alerted_at > 3600:
        _macro_alerted_at = now_ts
        send_message(f"🌍 MACRO RISK-OFF | {motivo} | ⛔ Pausando nuevas entradas")
        log.warning(f"🌍 MACRO RISK-OFF: {motivo}")

    return True, motivo


def get_vol_adjusted_size(base_size: float, strategy: str) -> tuple:
    """
    Escala el tamaño de la posición según la volatilidad reciente de los trades.
    vol_actual = std(pnl_pct de los últimos VOL_WINDOW trades cerrados)
    size_factor = min(1.0, VOL_TARGET / vol_actual)
    Con < 5 trades o mercado muy tranquilo devuelve factor 1.0 sin tocar base_size.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT pnl_pct FROM paper_trades
            WHERE status = 'CLOSED'
              AND COALESCE(strategy, 'STANDARD') = %s
              AND pnl_pct IS NOT NULL
            ORDER BY closed_at DESC
            LIMIT %s
        """, (strategy, VOL_WINDOW))
        rows = cur.fetchall()
        cur.close()
    finally:
        pool.putconn(conn)

    if len(rows) < 5:
        return base_size, 1.0

    # pnl_pct en DB está en % (ej. 30.5) → convertir a decimal para comparar con VOL_TARGET
    pnl_pcts = [float(r[0]) / 100.0 for r in rows]
    vol_actual = statistics.stdev(pnl_pcts)

    if vol_actual < 0.01:   # mercado casi sin varianza → no escalar
        return base_size, 1.0

    vol_factor = min(1.0, VOL_TARGET / vol_actual)
    adjusted   = round(base_size * vol_factor, 2)
    return adjusted, round(vol_factor, 3)


def has_concentration_risk(mint: str, strategy: str) -> tuple:
    """
    Devuelve (True, motivo) si abrir este token violaría límites de concentración:
    - Mismo deployer que un trade abierto → riesgo correlacionado
    - 2+ trades abiertos del mismo narrative en la misma estrategia
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        # Check deployer duplicado
        cur.execute("""
            SELECT COUNT(*) FROM paper_trades pt
            JOIN discovered_tokens dt  ON dt.mint  = pt.mint
            JOIN discovered_tokens dt2 ON dt2.mint = %s
            WHERE pt.status = 'OPEN'
              AND dt.deployer_wallet IS NOT NULL
              AND dt.deployer_wallet != ''
              AND dt.deployer_wallet = dt2.deployer_wallet
        """, (mint,))
        if cur.fetchone()[0] > 0:
            cur.close()
            return True, "mismo_deployer"

        # Check saturación de narrative (máx 2 por estrategia)
        cur.execute("""
            SELECT COUNT(*) FROM paper_trades pt
            JOIN discovered_tokens dt  ON dt.mint  = pt.mint
            JOIN discovered_tokens dt2 ON dt2.mint = %s
            WHERE pt.status = 'OPEN'
              AND COALESCE(pt.strategy, 'STANDARD') = %s
              AND dt2.narrative IS NOT NULL
              AND dt2.narrative != 'OTHER'
              AND dt.narrative = dt2.narrative
        """, (mint, strategy))
        if cur.fetchone()[0] >= 2:
            cur.close()
            return True, "mismo_narrative_x2"

        cur.close()
        return False, None
    finally:
        pool.putconn(conn)


def should_extend_tp(mint: str) -> bool:
    """True si el momentum actual justifica extender el TP: BSR > 3 y buys_5m > 20."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT buys_5m, sells_5m FROM discovered_tokens WHERE mint = %s
        """, (mint,))
        row = cur.fetchone()
        cur.close()
    finally:
        pool.putconn(conn)
    if not row:
        return False
    buys_5m, sells_5m = row
    bsr = (buys_5m or 0) / ((sells_5m or 0) + 1)
    return bsr > 3.0 and (buys_5m or 0) > 20


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
    # Fallback: si symbol es None usar name o prefijo del mint
    if not symbol:
        symbol = name[:12] if name else mint[:8]
    slip        = dynamic_slippage(trade_size, liquidity_usd)   # mejora 13
    entry_price = float(price) * (1 + slip + FEES)
    strat_stop  = STRATEGIES.get(strategy, {}).get('stop_loss')
    stop_pct    = strat_stop if strat_stop is not None else calc_adaptive_stop(mint)

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
              AND dt.market_cap >= 5000
              AND (
                dt.pair_address IS NULL                    -- bonding curve: sin par DexScreener
                OR dt.liquidity_usd >= 5000               -- con par: liquidez mínima (subido de 3k)
              )
              AND (dt.rug_flags IS NULL OR dt.rug_flags NOT LIKE '%%NEAR_ZERO_LIQUIDITY%%')
              AND (dt.price_change_1h IS NULL OR dt.price_change_1h > -70)
              AND (dt.narrative IS NULL OR dt.narrative NOT IN ('AI/AGI', 'IDENTITY', 'NUMBERS'))
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

def check_early_entry_signals():
    """
    Señales para EARLY_ENTRY: tokens con menos de EARLY_TOKEN_MAX_MINUTES minutos de vida,
    ML >= 65%, sin exigir entry_signal='ENTER' ni filtros de social/liquidez.
    Consulta directamente discovered_tokens independientemente del pipeline de entry_signal.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT dt.mint, dt.name, dt.symbol, dt.price_usd,
                   dt.ml_probability, dt.survival_score, dt.narrative,
                   dt.liquidity_usd
            FROM discovered_tokens dt
            WHERE dt.ml_probability >= 75
              AND dt.created_at > NOW() - INTERVAL '%s minutes'
              AND dt.price_usd IS NOT NULL
              AND dt.market_cap >= 1000
              AND (dt.rug_score IS NULL OR dt.rug_score < 40)
              AND dt.mint NOT IN (
                  SELECT mint FROM paper_trades
                  WHERE strategy = 'EARLY_ENTRY'
                    AND (status = 'OPEN'
                         OR opened_at > NOW() - INTERVAL '2 hours')
              )
            ORDER BY dt.ml_probability DESC
            LIMIT 5
        """ % EARLY_TOKEN_MAX_MINUTES)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def is_daily_limit_hit():
    cap = get_capital()
    return cap['daily_pnl'] / INITIAL_CAPITAL < -DAILY_LOSS_LIMIT

def is_trading_hours():
    """Solo abrir trades en ventanas de mayor win rate histórico.
    Excluye: sábados (WR 41%), horas 18–21h UTC (WR 35–41%).
    """
    now_utc = datetime.now(timezone.utc)
    if now_utc.weekday() == 5:  # 5 = sábado
        return False
    hour = now_utc.hour
    return any(start <= hour < end for start, end in TRADE_HOURS_UTC_WINDOWS)

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
        wins, losses, best_pct, worst_pct, day_pnl = row
        total = (wins or 0) + (losses or 0)
        win_rate = (wins / total * 100) if total > 0 else 0
        pnl_pct  = (float(cap['daily_pnl']) / INITIAL_CAPITAL) * 100

        # ── MISSED TRADES DEL DÍA ───────────────────────
        cur.execute("""
            SELECT
                COUNT(*)                                                   as total_missed,
                COUNT(*) FILTER (WHERE tracked_1h)                         as tracked,
                COALESCE(AVG(pnl_pct_1h) FILTER (WHERE tracked_1h), 0)    as avg_pnl_1h,
                COALESCE(SUM(phantom_pnl_1h) FILTER (WHERE tracked_1h), 0) as phantom_total,
                COUNT(*) FILTER (WHERE pnl_pct_1h > 0 AND tracked_1h)     as would_wins,
                MAX(pnl_pct_1h) FILTER (WHERE tracked_1h)                  as best_missed_pct,
                (ARRAY_AGG(name ORDER BY pnl_pct_1h DESC NULLS LAST))[1]  as best_missed_name,
                rejection_reason
            FROM missed_trades
            WHERE DATE(missed_at) = CURRENT_DATE - 1
            GROUP BY rejection_reason
            ORDER BY SUM(phantom_pnl_1h) DESC NULLS LAST
        """)
        missed_rows = cur.fetchall()
        cur.close()

        alert_daily_summary(
            capital   = cap['capital'],
            pnl       = cap['daily_pnl'],
            pnl_pct   = pnl_pct,
            wins      = wins or 0,
            losses    = losses or 0,
            best_trade= f"{float(best_pct):+.1f}%"
        )

        if missed_rows:
            from telegram_bot import alert_missed_summary
            alert_missed_summary(missed_rows)

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

def sync_paper_capital():
    """Mantiene paper_capital sincronizado como agregado de todas las estrategias."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE paper_capital SET
                capital    = (SELECT COALESCE(SUM(capital),   0) FROM strategy_capital),
                total_pnl  = (SELECT COALESCE(SUM(total_pnl), 0) FROM strategy_capital),
                wins       = (SELECT COALESCE(SUM(wins),      0) FROM strategy_capital),
                losses     = (SELECT COALESCE(SUM(losses),    0) FROM strategy_capital),
                updated_at = NOW()
        """)
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def rebalance_strategy_capital():
    """
    Redistribuye el capital total entre estrategias según su Sharpe ratio de los últimos 7 días.
    sharpe_7d = mean(daily_ret) / std(daily_ret) * sqrt(7)
    Pesos = softmax(sharpe_scores). Floor del 10% por estrategia.
    """
    if len(STRATEGIES) < 2:
        return

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        scaps         = {}
        sharpe_scores = {}
        for strat_name in STRATEGIES:
            scap = get_strategy_capital(strat_name)
            scaps[strat_name] = scap
            if not scap:
                sharpe_scores[strat_name] = 0.0
                continue
            cur.execute("""
                SELECT DATE(closed_at), SUM(pnl)
                FROM paper_trades
                WHERE status = 'CLOSED'
                  AND COALESCE(strategy, 'STANDARD') = %s
                  AND closed_at >= NOW() - INTERVAL '7 days'
                  AND pnl IS NOT NULL
                GROUP BY DATE(closed_at)
                ORDER BY 1
            """, (strat_name,))
            rows = cur.fetchall()
            if len(rows) < 3:
                # Datos insuficientes → Sharpe neutro (no penalizar ni premiar)
                sharpe_scores[strat_name] = 0.0
                continue
            daily_rets = [float(r[1]) / max(scap['initial_capital'], 1) for r in rows]
            mean_r = statistics.mean(daily_rets)
            std_r  = statistics.stdev(daily_rets) if len(daily_rets) > 1 else 0.0
            sharpe_scores[strat_name] = (
                mean_r / std_r * math.sqrt(7) if std_r > 1e-6
                else mean_r * math.sqrt(7)
            )
        cur.close()
    finally:
        pool.putconn(conn)

    strat_names   = list(STRATEGIES.keys())
    scores        = [sharpe_scores[s] for s in strat_names]
    total_capital = sum(scaps[s]['capital'] for s in strat_names if scaps.get(s))

    if total_capital <= 0:
        return

    # Softmax con shift numérico para evitar overflow
    max_s      = max(scores)
    exp_scores = [math.exp(s - max_s) for s in scores]
    total_exp  = sum(exp_scores)
    weights    = [e / total_exp for e in exp_scores]

    min_alloc = total_capital * 0.10   # floor: ninguna estrategia cae por debajo del 10%

    log.info(f"📊 REBALANCEO SEMANAL | Capital total: ${total_capital:.2f}")
    lines = [f"📊 Rebalanceo semanal | Total: ${total_capital:.2f}"]
    for strat, weight, sharpe in zip(strat_names, weights, scores):
        scap = scaps.get(strat)
        if not scap:
            continue
        old_cap = scap['capital']
        new_cap = max(min_alloc, total_capital * weight)
        update_strategy_capital(strat, new_cap, scap['wins'], scap['losses'], scap['total_pnl'])
        arrow = "↑" if new_cap > old_cap + 0.01 else ("↓" if new_cap < old_cap - 0.01 else "=")
        log.info(f"  [{strat}] Sharpe7d={sharpe:+.2f} | peso={weight*100:.1f}% | "
                 f"${old_cap:.2f} {arrow} ${new_cap:.2f}")
        lines.append(f"  {strat}: Sharpe={sharpe:+.2f} | {weight*100:.0f}% | "
                     f"${old_cap:.0f}{arrow}${new_cap:.0f}")

    sync_paper_capital()
    send_message("\n".join(lines))
    log.info("✅ Rebalanceo completado")


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
                scap_row = get_strategy_capital(strat_name)
                s_cap    = scap_row['capital'] if scap_row else scfg['initial_capital']
                s_init   = scfg['initial_capital']
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

def _apply_pnl_to_cap(strategy, pnl, trade_size, is_win):
    """Actualiza strategy_capital para la estrategia dada y sincroniza paper_capital."""
    scap = get_strategy_capital(strategy)
    if scap:
        scap['capital']   += float(trade_size) + pnl
        scap['total_pnl'] += pnl
        scap['wins' if is_win else 'losses'] += 1
        update_strategy_capital(strategy, scap['capital'],
                                scap['wins'], scap['losses'], scap['total_pnl'])
    sync_paper_capital()

def _apply_partial_pnl(strategy, pnl, half_size):
    """Actualiza capital y PnL para partial TP sin contar como win/loss individual."""
    scap = get_strategy_capital(strategy)
    if scap:
        scap['capital']   += float(half_size) + pnl
        scap['total_pnl'] += pnl
        update_strategy_capital(strategy, scap['capital'],
                                scap['wins'], scap['losses'], scap['total_pnl'])
    sync_paper_capital()

# Contador de ciclos sin precio por trade_id (en memoria)
_no_price_count: dict[int, int] = {}
# Trades con TP extendido por momentum fuerte (en memoria)
_tp_extended: set = set()
# Máx ciclos sin precio antes de cerrar al stop máximo (6 × 10s = 60s)
NO_PRICE_LIMIT = 6


def manage_open_trades():
    """Revisa y gestiona todas las posiciones abiertas. Llamado cada 10s."""
    open_trades = get_open_trades()
    for trade in open_trades:
        tid, mint, name, symbol, entry_price, trade_size, \
            ml_prob, score, opened_at, peak_price, stop_loss_pct, strategy, partial_tp_taken = trade

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
                _apply_pnl_to_cap(strategy, pnl, trade_size, False)
                _no_price_count.pop(tid, None)
            continue
        _no_price_count.pop(tid, None)  # reset al recuperar precio

        entry       = float(entry_price)
        peak        = float(peak_price) if peak_price else entry
        age_minutes = (datetime.now() - opened_at.replace(tzinfo=None)).total_seconds() / 60

        # Actualizar precio pico
        if current_price > peak:
            peak = current_price
            update_peak_price(tid, peak)

        # ── PARTIAL TAKE PROFIT (+30%: cerrar 50%) ─────────
        # Al llegar a +30%, aseguramos la mitad de la posición.
        # El 50% restante sigue hasta TAKE_PROFIT (+80%), STOP_LOSS o TIMEOUT.
        # (trailing stop desactivado 2026-04-10 — análisis 106 trades)
        if not partial_tp_taken and current_price >= entry * PARTIAL_TP_PCT:
            half_size = float(trade_size) / 2.0
            exit_price_partial = current_price * (1 - FEES)
            pnl_pct_partial = (exit_price_partial - entry) / entry * 100
            pnl_partial = half_size * (pnl_pct_partial / 100)

            conn_p = pool.getconn()
            try:
                cur_p = conn_p.cursor()
                cur_p.execute("""
                    UPDATE paper_trades SET
                        trade_size       = trade_size / 2.0,
                        partial_tp_taken = TRUE
                    WHERE id = %s
                """, (tid,))
                conn_p.commit()
                cur_p.close()
            finally:
                pool.putconn(conn_p)

            log.info(f"💰 PARTIAL_TP #{tid} [{strategy}] | {name or mint[:8]} | "
                     f"+{pnl_pct_partial:.1f}% | 50% cerrado: ${pnl_partial:+.2f} | "
                     f"Restante: ${half_size:.2f} | Trailing activo desde ahora")
            if strategy == 'STANDARD':
                send_message(
                    f"💰 PARTIAL TP | {name or mint[:8]} | "
                    f"+{pnl_pct_partial:.1f}% | ${pnl_partial:+.2f} asegurado | "
                    f"Restante ${half_size:.2f} en trailing"
                )
            _apply_partial_pnl(strategy, pnl_partial, half_size)
            # Actualizar variables locales para los checks siguientes
            partial_tp_taken = True
            trade_size = half_size

        # ── EXTENDED TAKE PROFIT (momentum extendió el TP previamente) ──
        ext_tp_level = TAKE_PROFIT + EXTENDED_TP_BOOST
        if tid in _tp_extended and current_price >= entry * ext_tp_level:
            pnl, pnl_pct = close_trade(
                tid, mint, name, entry_price,
                float(trade_size), current_price, "TAKE_PROFIT_EXT", strategy
            )
            _apply_pnl_to_cap(strategy, pnl, trade_size, pnl > 0)
            _tp_extended.discard(tid)

        # ── TAKE PROFIT base ────────────────────────────────
        elif current_price >= entry * TAKE_PROFIT:
            if tid not in _tp_extended and should_extend_tp(mint):
                # Momentum fuerte → extender TP una vez
                _tp_extended.add(tid)
                log.info(f"🚀 TP EXTENDIDO #{tid} [{strategy}] | {name or mint[:8]} | "
                         f"BSR>3 | nuevo TP +{(ext_tp_level-1)*100:.0f}%")
            elif tid not in _tp_extended:
                # Momentum débil → cerrar al TP normal
                pnl, pnl_pct = close_trade(
                    tid, mint, name, entry_price,
                    float(trade_size), current_price, "TAKE_PROFIT", strategy
                )
                _apply_pnl_to_cap(strategy, pnl, trade_size, pnl > 0)
            # else: en modo extendido, esperando ext_tp_level — no hacer nada

        # ── EMERGENCY EXIT (<5min, precio cae >50%) ──────────
        elif (age_minutes < EMERGENCY_EXIT_MINUTES and
              current_price < entry * EMERGENCY_EXIT_PCT):
            sim_exit = entry * EMERGENCY_EXIT_PCT
            log.warning(f"🚨 EMERGENCY EXIT #{tid} [{strategy}] | {name or mint[:8]} | "
                        f"age={age_minutes:.1f}min | caída real={((current_price/entry)-1)*100:+.1f}% | "
                        f"salida simulada al -50%: ${sim_exit:.8f}")
            pnl, pnl_pct = close_trade(
                tid, mint, name, entry_price,
                float(trade_size), sim_exit, "EMERGENCY_EXIT", strategy
            )
            _apply_pnl_to_cap(strategy, pnl, trade_size, False)
            _tp_extended.discard(tid)

        # ── STOP LOSS ADAPTATIVO ────────────────────────────
        elif current_price <= entry * float(stop_loss_pct):
            pnl, pnl_pct = close_trade(
                tid, mint, name, entry_price,
                float(trade_size), current_price, "STOP_LOSS", strategy
            )
            _apply_pnl_to_cap(strategy, pnl, trade_size, False)
            _tp_extended.discard(tid)

        # ── TIMEOUT ─────────────────────────────────────────
        elif (datetime.now() - opened_at.replace(tzinfo=None) >
              timedelta(minutes=MAX_HOLD_MINUTES)):
            pnl, pnl_pct = close_trade(
                tid, mint, name, entry_price,
                float(trade_size), current_price, "TIMEOUT", strategy
            )
            _apply_pnl_to_cap(strategy, pnl, trade_size, pnl > 0)
            _tp_extended.discard(tid)


_monitoring_alerted: dict = {}   # throttle por tipo de alerta

def _check_monitoring():
    """
    Checks periódicos de salud del sistema. Llamado cada hora desde run().
    - 5 trades consecutivos perdedores por estrategia → alerta
    - Sin muestras ML limpias en 6h → alerta
    - Drawdown 10-15% → alerta amarilla (>15% ya cubierto en get_drawdown_factor)
    """
    now_ts = time.monotonic()

    # ── Racha perdedora ────────────────────────────────
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        for strat_name in STRATEGIES:
            cur.execute("""
                SELECT pnl > 0 FROM paper_trades
                WHERE status = 'CLOSED'
                  AND COALESCE(strategy, 'STANDARD') = %s
                ORDER BY closed_at DESC LIMIT 5
            """, (strat_name,))
            results = [r[0] for r in cur.fetchall()]
            if len(results) == 5 and not any(results):
                key = f"loss_streak_{strat_name}"
                if now_ts - _monitoring_alerted.get(key, 0) > 3600:
                    _monitoring_alerted[key] = now_ts
                    alert_consecutive_losses(5, strat_name)
                    log.warning(f"🔴 [{strat_name}] 5 trades consecutivos en pérdida")

        # ── Sin muestras ML limpias ────────────────────
        cur.execute("""
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(captured_at))) / 3600
            FROM token_features_at_signal
        """)
        row = cur.fetchone()
        if row and row[0] is not None:
            hours_since = float(row[0])
            if hours_since > 6.0:
                key = "no_ml_samples"
                if now_ts - _monitoring_alerted.get(key, 0) > 3600:
                    _monitoring_alerted[key] = now_ts
                    alert_no_ml_samples(hours_since)
                    log.warning(f"⚠️  Sin muestras ML: {hours_since:.1f}h")

        # ── Drawdown amarillo (10-15%) ─────────────────
        for strat_name in STRATEGIES:
            scap = get_strategy_capital(strat_name)
            if not scap:
                continue
            peak = scap['peak_capital']
            if peak <= 0:
                continue
            dd = (peak - scap['capital']) / peak
            if 0.10 <= dd < 0.15:
                key = f"dd_yellow_{strat_name}"
                if now_ts - _monitoring_alerted.get(key, 0) > 3600:
                    _monitoring_alerted[key] = now_ts
                    send_message(
                        f"⚠️ DRAWDOWN AMARILLO [{strat_name}] | "
                        f"{dd*100:.1f}% desde pico ${peak:.2f} → ${scap['capital']:.2f}"
                    )
                    log.warning(f"⚠️  [{strat_name}] Drawdown amarillo: {dd*100:.1f}%")

        cur.close()
    except Exception as e:
        log.warning(f"_check_monitoring error: {e}")
    finally:
        pool.putconn(conn)


def run():
    setup_db()
    sync_paper_capital()   # Sincronizar al arrancar por si hay deriva acumulada
    log.info("💰 Paper Trading Engine arrancando...")
    log.info(f"⏱  Ciclo posiciones: {POSITION_CHECK_INTERVAL}s | Ciclo señales: {SIGNAL_CHECK_INTERVAL}s")
    alert_system_status("OK", "Paper Trading Engine iniciado")

    last_daily_reset  = datetime.now().date()
    last_signal_check = 0.0
    last_summary      = 0.0
    last_rebalance    = datetime.now().date()
    last_monitoring   = 0.0   # check horario de salud del sistema

    while True:
        try:
            now = time.monotonic()

            # ── RESET DIARIO ───────────────────────────────
            today = datetime.now().date()
            if today > last_daily_reset:
                send_daily_summary()
                reset_daily_pnl()
                last_daily_reset = today

            # ── REBALANCEO SEMANAL ──────────────────────────
            if (datetime.now().date() - last_rebalance).days >= 7:
                rebalance_strategy_capital()
                last_rebalance = datetime.now().date()

            # ── MONITORING HORARIO ──────────────────────────
            if now - last_monitoring >= 3600:
                last_monitoring = now
                _check_monitoring()

            # ── GESTIONAR POSICIONES ABIERTAS (cada 10s) ───
            # SIEMPRE gestionar posiciones abiertas, incluso con límite diario activo
            manage_open_trades()

            # ── LÍMITE DIARIO — bloquea solo apertura de nuevos trades ──
            if is_daily_limit_hit():
                log.warning("⛔ Límite de pérdida diaria alcanzado — no se abren nuevos trades")
                time.sleep(POSITION_CHECK_INTERVAL)
                continue

            # ── ABRIR NUEVOS TRADES (cada 60s) ─────────────
            if now - last_signal_check >= SIGNAL_CHECK_INTERVAL:
                last_signal_check = now
                if is_trading_hours():
                    # ── FILTRO MACRO: pausar si SOL/BTC cae bruscamente ──────
                    macro_off, macro_reason = get_macro_risk()
                    if macro_off:
                        log.warning(f"🌍 MACRO RISK-OFF ({macro_reason}) — ciclo saltado")
                        continue

                    # ── Estrategias standard (requieren entry_signal='ENTER') ──
                    signals = check_new_signals(ml_min=80)
                    for sig in signals:
                        mint, name, symbol, price, ml_prob, score, narrative, liquidity = sig
                        if not price:
                            continue
                        for strat_name, scfg in STRATEGIES.items():
                            if scfg['signal_source'] != 'standard':
                                continue
                            if (ml_prob or 0) < scfg['ml_min']:
                                log_missed_trade(
                                    mint, name, symbol, ml_prob, 'PAPER_TRADING',
                                    'ML_BELOW_STRATEGY',
                                    f'ml={ml_prob}% < {scfg["ml_min"]}% ({strat_name})',
                                    price, strategy=strat_name
                                )
                                continue
                            open_count = len(get_open_trades(strategy=strat_name))
                            if open_count >= scfg['max_open']:
                                log_missed_trade(
                                    mint, name, symbol, ml_prob, 'PAPER_TRADING',
                                    'MAX_OPEN',
                                    f'{strat_name}: {open_count}/{scfg["max_open"]} trades abiertos',
                                    price, strategy=strat_name
                                )
                                continue
                            scap = get_strategy_capital(strat_name)
                            if not scap:
                                continue
                            dd_factor = get_drawdown_factor(strat_name)
                            if dd_factor == 0.0:
                                log_missed_trade(
                                    mint, name, symbol, ml_prob, 'PAPER_TRADING',
                                    'DRAWDOWN_PAUSE',
                                    f'{strat_name}: drawdown >15% — entradas pausadas',
                                    price, strategy=strat_name
                                )
                                continue
                            conc_risk, conc_reason = has_concentration_risk(mint, strat_name)
                            if conc_risk:
                                log_missed_trade(
                                    mint, name, symbol, ml_prob, 'PAPER_TRADING',
                                    'CONCENTRATION_RISK', conc_reason,
                                    price, strategy=strat_name
                                )
                                log.info(f"🚫 [{strat_name}] CONCENTRATION_RISK: {conc_reason} — {name or mint[:8]}")
                                continue
                            if scfg['size_pct'] is None:
                                t_size, size_pct = kelly_size(ml_prob or 65, scap['capital'])
                                t_size = round(t_size * dd_factor, 2)
                                base_label = f"Kelly {size_pct:.1f}%"
                            else:
                                t_size = round(scap['capital'] * scfg['size_pct'] * dd_factor, 2)
                                base_label = f"Fixed {scfg['size_pct']*100:.0f}%"
                            t_size, vol_factor = get_vol_adjusted_size(t_size, strat_name)
                            vol_tag = f" × VOL{vol_factor}" if vol_factor < 1.0 else ""
                            log.info(f"💰 [{strat_name}] {base_label} × DD{dd_factor}{vol_tag} = ${t_size:.2f} | ML={ml_prob}%")
                            open_trade(mint, name, symbol, price,
                                       ml_prob, score, narrative, t_size,
                                       strategy=strat_name, liquidity_usd=liquidity)
                            scap = get_strategy_capital(strat_name)
                            if scap:
                                scap['capital'] -= t_size
                                update_strategy_capital(strat_name, scap['capital'],
                                                        scap['wins'], scap['losses'], scap['total_pnl'])

                else:
                    hour = datetime.now(timezone.utc).hour
                    log.debug(f"🌙 Fuera de horario ({hour}h UTC) — no se abren nuevos trades")
                    # Log missed trades durante horas fuera de ventana
                    outside_signals = check_new_signals(ml_min=80)
                    for sig in outside_signals:
                        mint, name, symbol, price, ml_prob, score, narrative, liquidity = sig
                        if price:
                            log_missed_trade(
                                mint, name, symbol, ml_prob, 'PAPER_TRADING',
                                'OUTSIDE_HOURS',
                                f'hora={hour}h UTC fuera de {TRADE_HOURS_UTC_WINDOWS}',
                                price, strategy='ALL'
                            )

                if now - last_summary >= SIGNAL_CHECK_INTERVAL:
                    last_summary = now
                    print_summary()

            time.sleep(POSITION_CHECK_INTERVAL)

        except Exception as e:
            log.error(f"Error en paper trading: {e}")
            time.sleep(30)

if __name__ == "__main__":
    run()
