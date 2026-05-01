import asyncio
import aiohttp
import psycopg2
import psycopg2.pool
import os
import logging
import pickle
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/entry_signal.log'),
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

MODEL_PATH = '/root/solana_bot/model.pkl'
FEATURES_PATH = '/root/solana_bot/features.pkl'
ML_THRESHOLD      = 0.35    # Probabilidad mínima para ENTER (calibrada: base rate ~23%)
RUG_THRESHOLD     = 60     # Rug score máximo permitido
MIN_LIQUIDITY_USD = 3_000  # Liquidez mínima en USD para abrir trade

# ── CARGAR MODELO ML ──────────────────────────────────
def load_ml_model():
    try:
        with open(MODEL_PATH, 'rb') as f:
            model = pickle.load(f)
        with open(FEATURES_PATH, 'rb') as f:
            saved = pickle.load(f)
        if isinstance(model, dict):
            n = len(model.get('models', []))
            w = [f"{x:.3f}" for x in model.get('weights', [])]
            log.info(f"✅ Ensemble ML cargado ({n} modelos, pesos={w})")
        else:
            log.info("✅ Modelo ML cargado (single model)")
        return model, saved['label_encoder']
    except Exception as e:
        log.warning(f"⚠️  Modelo ML no disponible: {e} — usando reglas manuales")
        return None, None

ml_model, label_encoder = load_ml_model()

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS price_snapshots (
                id SERIAL PRIMARY KEY,
                mint TEXT REFERENCES discovered_tokens(mint),
                price_usd NUMERIC(20,10),
                market_cap NUMERIC(20,2),
                volume NUMERIC(20,2),
                buys INTEGER,
                sells INTEGER,
                snapshot_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_snapshots_mint ON price_snapshots(mint);
            CREATE INDEX IF NOT EXISTS idx_snapshots_time ON price_snapshots(snapshot_at DESC);
        """)
        conn.commit()
        try:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("""
                ALTER TABLE discovered_tokens
                ADD COLUMN IF NOT EXISTS entry_signal TEXT DEFAULT 'WAIT',
                ADD COLUMN IF NOT EXISTS entry_price NUMERIC(20,10),
                ADD COLUMN IF NOT EXISTS entry_at TIMESTAMP,
                ADD COLUMN IF NOT EXISTS price_change_15m NUMERIC(10,2),
                ADD COLUMN IF NOT EXISTS price_change_1h NUMERIC(10,2),
                ADD COLUMN IF NOT EXISTS ml_probability NUMERIC(5,2)
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"ALTER TABLE skipped (lock timeout): {e}")
        cur.close()
        log.info("✅ Tablas de señales creadas")
    finally:
        pool.putconn(conn)

def log_missed_trade(mint, name, symbol, ml_prob, stage, reason, detail, price):
    """Registra un trade que fue rechazado por algún filtro. Nunca lanza excepción."""
    try:
        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO missed_trades
                    (mint, name, symbol, ml_probability, rejection_stage,
                     rejection_reason, rejection_detail, entry_price)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM missed_trades
                    WHERE mint = %s AND rejection_reason = %s
                      AND missed_at > NOW() - INTERVAL '2 hours'
                )
            """, (mint, name, symbol, ml_prob, stage, reason, detail, price,
                  mint, reason))
            conn.commit()
            cur.close()
        finally:
            pool.putconn(conn)
    except Exception as e:
        log.warning(f"log_missed_trade error: {e}")

async def fetch_current_data(session, mint):
    # Fuente 1: DexScreener (datos completos)
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data = await resp.json()
            pairs = data.get("pairs")
            if pairs:
                pair = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
                price = pair.get("priceUsd")
                if price:
                    return {
                        "price_usd":       price,
                        "market_cap":      pair.get("marketCap"),
                        "volume":          pair.get("volume", {}).get("h24", 0),
                        "buys_1h":         pair.get("txns", {}).get("h1", {}).get("buys", 0),
                        "sells_1h":        pair.get("txns", {}).get("h1", {}).get("sells", 0),
                        "buys_5m":         pair.get("txns", {}).get("m5", {}).get("buys", 0),
                        "sells_5m":        pair.get("txns", {}).get("m5", {}).get("sells", 0),
                        "price_change_5m": pair.get("priceChange", {}).get("m5", 0),
                        "price_change_1h": pair.get("priceChange", {}).get("h1", 0),
                    }
    except Exception:
        pass

    # Fuente 2 (fallback): Jupiter Price API (solo precio)
    try:
        url = f"https://api.jup.ag/price/v2?ids={mint}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            data = await resp.json()
            price_data = data.get("data", {}).get(mint)
            if price_data and price_data.get("price"):
                log.debug(f"Jupiter fallback para {mint[:8]}")
                return {
                    "price_usd":       str(price_data["price"]),
                    "market_cap":      None,
                    "volume":          0,
                    "buys_1h":         0, "sells_1h":  0,
                    "buys_5m":         0, "sells_5m":  0,
                    "price_change_5m": 0, "price_change_1h": 0,
                }
    except Exception:
        pass

    return None

def save_snapshot(mint, data):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO price_snapshots (mint, price_usd, market_cap, volume, buys, sells)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            mint, data["price_usd"], data["market_cap"],
            data["volume"], data["buys_5m"], data["sells_5m"]
        ))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_price_history(mint):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT price_usd, market_cap, volume, buys, sells, snapshot_at
            FROM price_snapshots
            WHERE mint = %s
            ORDER BY snapshot_at DESC
            LIMIT 10
        """, (mint,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def get_token_features(mint):
    """Obtiene todas las features del token para el modelo ML"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT buys_5m, sells_5m, market_cap, volume_24h,
                   COALESCE(volume_1h, 0)              as volume_1h,
                   COALESCE(volume_5m, 0)              as volume_5m,
                   survival_score, narrative, buys_1h, sells_1h,
                   COALESCE(buys_24h, 0)               as buys_24h,
                   COALESCE(sells_24h, 0)              as sells_24h,
                   COALESCE(rug_score, 50)             as rug_score,
                   COALESCE(holder_count, 10)          as holder_count,
                   COALESCE(top10_concentration, 95)   as top10_concentration,
                   COALESCE(dev_sold, FALSE)           as dev_sold,
                   (twitter  IS NOT NULL)              as has_twitter,
                   (telegram IS NOT NULL)              as has_telegram,
                   (website  IS NOT NULL)              as has_website,
                   EXTRACT(HOUR FROM created_at)       as launch_hour,
                   COALESCE(liquidity_usd, 0)          as liquidity_usd,
                   COALESCE(fdv, 0)                    as fdv,
                   -- Smart money
                   CASE WHEN EXISTS (
                       SELECT 1 FROM wallet_activity wa
                       JOIN smart_wallets sw ON sw.wallet = wa.wallet
                       WHERE wa.mint = dt.mint AND sw.is_smart = TRUE
                   ) THEN 1 ELSE 0 END                as smart_money_bought,
                   -- Narrative momentum
                   COALESCE((
                       SELECT ns.momentum FROM narrative_stats ns
                       WHERE ns.narrative = dt.narrative
                         AND ns.window_hours = 24
                       ORDER BY ns.calculated_at DESC LIMIT 1
                   ), 0)                              as narrative_momentum,
                   -- Historial del deployer
                   COALESCE(ds.total_tokens, 0)       as deployer_prior_tokens,
                   COALESCE(ds.rugged_count, 0)       as deployer_rugged_count,
                   COALESCE(ds.rug_rate, 0.5)         as deployer_rug_rate,
                   COALESCE(ds.is_serial_rugger::int, 0) as is_known_rugger,
                   (dt.deployer_wallet IS NOT NULL)::int as deployer_known
            FROM discovered_tokens dt
            LEFT JOIN deployer_stats ds ON ds.wallet = dt.deployer_wallet
            WHERE dt.mint = %s
        """, (mint,))
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        pool.putconn(conn)

def ml_predict(mint):
    """
    Retorna (probabilidad, es_valido) usando el modelo ML.
    Fallback a reglas manuales si el modelo no está disponible.
    """
    if ml_model is None:
        return None, True

    row = get_token_features(mint)
    if not row:
        return None, True

    try:
        df = pd.DataFrame([row], columns=[
            'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
            'volume_1h', 'volume_5m', 'survival_score', 'narrative',
            'buys_1h', 'sells_1h', 'buys_24h', 'sells_24h',
            'rug_score', 'holder_count', 'top10_concentration',
            'dev_sold', 'has_twitter', 'has_telegram', 'has_website',
            'launch_hour', 'liquidity_usd', 'fdv',
            'smart_money_bought', 'narrative_momentum',
            'deployer_prior_tokens', 'deployer_rugged_count',
            'deployer_rug_rate', 'is_known_rugger', 'deployer_known',
        ])
        numeric_cols = [c for c in df.columns if c != 'narrative']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        # Feature engineering — debe coincidir exactamente con ml_model.py
        df['buy_sell_ratio']     = df['buys_5m']  / (df['sells_5m']  + 1)
        df['buy_sell_ratio_1h']  = df['buys_1h']  / (df['sells_1h']  + 1)
        df['buy_sell_ratio_24h'] = df['buys_24h'] / (df['sells_24h'] + 1)
        df['net_buy_pressure']   = df['buys_5m'] - df['sells_5m']
        df['vol_to_mcap']        = df['volume_24h'] / (df['market_cap'] + 1)
        df['vol5m_to_mcap']      = df['volume_5m']  / (df['market_cap'] + 1)
        df['log_mcap']           = np.log1p(df['market_cap'])
        df['log_volume']         = np.log1p(df['volume_24h'])
        df['log_volume_1h']      = np.log1p(df['volume_1h'])
        df['log_volume_5m']      = np.log1p(df['volume_5m'])
        df['score_normalized']   = df['survival_score'] / 100.0
        df['holder_risk']        = df['top10_concentration'] / 100.0
        df['zero_sells']         = (df['sells_5m'] == 0).astype(int)
        df['dev_sold']           = df['dev_sold'].astype(int)
        df['has_twitter']        = df['has_twitter'].astype(int)
        df['has_telegram']       = df['has_telegram'].astype(int)
        df['has_website']        = df['has_website'].astype(int)
        df['optimal_mcap']       = ((df['market_cap'] >= 1000) & (df['market_cap'] <= 10000)).astype(int)
        df['peak_hours']         = df['launch_hour'].apply(lambda h: 1 if 14 <= h <= 22 else 0)
        df['log_liquidity']      = np.log1p(df['liquidity_usd'])
        df['liquidity_to_mcap']  = df['liquidity_usd'] / (df['market_cap'] + 1)
        df['has_liquidity']      = (df['liquidity_usd'] > 1000).astype(int)
        df['log_fdv']            = np.log1p(df['fdv'])
        df['fdv_to_mcap']        = df['fdv'] / (df['market_cap'] + 1)

        # Velocidad de actividad
        df['buys_growth']      = df['buys_5m']  / (df['buys_24h']  / 288.0 + 1)
        df['sells_growth']     = df['sells_5m'] / (df['sells_24h'] / 288.0 + 1)
        df['buy_acceleration'] = df['buys_1h']  / (df['buys_24h']  / 24.0  + 1)
        df['vol_growth_1h']    = df['volume_1h'] / (df['volume_24h'] / 24.0  + 1)
        df['vol_growth_5m']    = df['volume_5m'] / (df['volume_24h'] / 288.0 + 1)

        # Smart money y narrativa
        df['smart_money_bought'] = pd.to_numeric(df['smart_money_bought'], errors='coerce').fillna(0).astype(int)
        df['narrative_momentum'] = pd.to_numeric(df['narrative_momentum'], errors='coerce').fillna(0)

        # Deployer features
        df['deployer_prior_tokens'] = np.log1p(pd.to_numeric(df['deployer_prior_tokens'], errors='coerce').fillna(0))
        df['deployer_rugged_count'] = pd.to_numeric(df['deployer_rugged_count'], errors='coerce').fillna(0).astype(int)
        df['deployer_rug_rate']     = pd.to_numeric(df['deployer_rug_rate'], errors='coerce').fillna(0.5).clip(0, 1)
        df['is_known_rugger']       = pd.to_numeric(df['is_known_rugger'], errors='coerce').fillna(0).astype(int)
        df['deployer_known']        = pd.to_numeric(df['deployer_known'], errors='coerce').fillna(0).astype(int)

        try:
            df['narrative_encoded'] = label_encoder.transform(df['narrative'].fillna('OTHER'))
        except Exception:
            df['narrative_encoded'] = 0

        features = [
            'buys_5m', 'sells_5m', 'market_cap', 'volume_24h',
            'volume_1h', 'volume_5m', 'survival_score',
            'buys_1h', 'sells_1h', 'buys_24h', 'sells_24h',
            'rug_score', 'holder_count', 'top10_concentration',
            'launch_hour',
            'buy_sell_ratio', 'buy_sell_ratio_1h', 'buy_sell_ratio_24h',
            'net_buy_pressure', 'vol_to_mcap', 'vol5m_to_mcap',
            'log_mcap', 'log_volume', 'log_volume_1h', 'log_volume_5m',
            'score_normalized', 'holder_risk',
            'narrative_encoded',
            'zero_sells', 'optimal_mcap', 'peak_hours',
            'dev_sold', 'has_twitter', 'has_telegram', 'has_website',
            'has_liquidity',
            'liquidity_usd', 'log_liquidity', 'liquidity_to_mcap',
            'fdv', 'log_fdv', 'fdv_to_mcap',
            # Velocidad
            'buys_growth', 'sells_growth', 'buy_acceleration',
            'vol_growth_1h', 'vol_growth_5m',
            # Señales externas
            'smart_money_bought', 'narrative_momentum',
            # Historial del deployer
            'deployer_prior_tokens', 'deployer_rugged_count',
            'deployer_rug_rate', 'is_known_rugger', 'deployer_known',
        ]

        X = df[features].fillna(0).values
        if isinstance(ml_model, dict):
            proba = sum(
                w * float(m.predict_proba(X)[0][1])
                for m, w in zip(ml_model['models'], ml_model['weights'])
            )
        else:
            proba = float(ml_model.predict_proba(X)[0][1])
        return round(proba * 100, 1), proba >= ML_THRESHOLD

    except Exception as e:
        log.warning(f"ML predict error para {mint[:8]}: {e}")
        return None, True

def capture_features_at_enter(mint):
    """
    Captura features al momento exacto del ENTER signal — después de que DexScreener
    tiene un pair y narrative_engine ha clasificado el token.
    DO UPDATE sobrescribe registros anteriores con liquidity_usd=0 capturados al BUY.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO token_features_at_signal (
                mint, signal, captured_at, price_at_signal,
                buys_5m, sells_5m, market_cap, volume_24h, volume_1h, volume_5m,
                survival_score, narrative, buys_1h, sells_1h, buys_24h, sells_24h,
                rug_score, holder_count, top10_concentration, dev_sold,
                has_twitter, has_telegram, has_website, launch_hour,
                liquidity_usd, fdv, smart_money_bought, narrative_momentum,
                deployer_prior_tokens, deployer_rugged_count, deployer_rug_rate,
                is_known_rugger, deployer_known
            )
            SELECT
                dt.mint, 'ENTER', NOW(), dt.price_usd,
                dt.buys_5m, dt.sells_5m, dt.market_cap, dt.volume_24h,
                COALESCE(dt.volume_1h, 0), COALESCE(dt.volume_5m, 0),
                dt.survival_score, dt.narrative, dt.buys_1h, dt.sells_1h,
                COALESCE(dt.buys_24h, 0), COALESCE(dt.sells_24h, 0),
                COALESCE(dt.rug_score, 50), COALESCE(dt.holder_count, 10),
                COALESCE(dt.top10_concentration, 95), COALESCE(dt.dev_sold, FALSE),
                (dt.twitter IS NOT NULL), (dt.telegram IS NOT NULL), (dt.website IS NOT NULL),
                EXTRACT(HOUR FROM dt.created_at),
                COALESCE(dt.liquidity_usd, 0), COALESCE(dt.fdv, 0),
                CASE WHEN EXISTS (
                    SELECT 1 FROM wallet_activity wa
                    JOIN smart_wallets sw ON sw.wallet = wa.wallet
                    WHERE wa.mint = dt.mint AND sw.is_smart = TRUE
                ) THEN 1 ELSE 0 END,
                COALESCE((
                    SELECT ns.momentum FROM narrative_stats ns
                    WHERE ns.narrative = dt.narrative AND ns.window_hours = 24
                    ORDER BY ns.calculated_at DESC LIMIT 1
                ), 0),
                COALESCE(ds.total_tokens, 0), COALESCE(ds.rugged_count, 0),
                COALESCE(ds.rug_rate, 0.5), COALESCE(ds.is_serial_rugger::int, 0),
                (dt.deployer_wallet IS NOT NULL)::int
            FROM discovered_tokens dt
            LEFT JOIN deployer_stats ds ON ds.wallet = dt.deployer_wallet
            WHERE dt.mint = %s
            ON CONFLICT (mint) DO UPDATE SET
                signal                = EXCLUDED.signal,
                captured_at           = EXCLUDED.captured_at,
                price_at_signal       = EXCLUDED.price_at_signal,
                buys_5m               = EXCLUDED.buys_5m,
                sells_5m              = EXCLUDED.sells_5m,
                market_cap            = EXCLUDED.market_cap,
                volume_24h            = EXCLUDED.volume_24h,
                volume_1h             = EXCLUDED.volume_1h,
                volume_5m             = EXCLUDED.volume_5m,
                survival_score        = EXCLUDED.survival_score,
                narrative             = EXCLUDED.narrative,
                buys_1h               = EXCLUDED.buys_1h,
                sells_1h              = EXCLUDED.sells_1h,
                buys_24h              = EXCLUDED.buys_24h,
                sells_24h             = EXCLUDED.sells_24h,
                rug_score             = EXCLUDED.rug_score,
                holder_count          = EXCLUDED.holder_count,
                top10_concentration   = EXCLUDED.top10_concentration,
                dev_sold              = EXCLUDED.dev_sold,
                has_twitter           = EXCLUDED.has_twitter,
                has_telegram          = EXCLUDED.has_telegram,
                has_website           = EXCLUDED.has_website,
                launch_hour           = EXCLUDED.launch_hour,
                liquidity_usd         = EXCLUDED.liquidity_usd,
                fdv                   = EXCLUDED.fdv,
                smart_money_bought    = EXCLUDED.smart_money_bought,
                narrative_momentum    = EXCLUDED.narrative_momentum,
                deployer_prior_tokens = EXCLUDED.deployer_prior_tokens,
                deployer_rugged_count = EXCLUDED.deployer_rugged_count,
                deployer_rug_rate     = EXCLUDED.deployer_rug_rate,
                is_known_rugger       = EXCLUDED.is_known_rugger,
                deployer_known        = EXCLUDED.deployer_known
        """, (mint,))
        conn.commit()
        cur.close()
    except Exception as e:
        log.warning(f"capture_features_at_enter falló para {mint[:8]}: {e}")
        conn.rollback()
    finally:
        pool.putconn(conn)

def is_rug_safe(mint):
    """Retorna True si el token pasa el filtro de rug"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT rug_score FROM discovered_tokens WHERE mint = %s", (mint,))
        row = cur.fetchone()
        cur.close()
        if not row or row[0] is None:
            return True  # Sin datos = no bloqueamos
        return (row[0] or 0) < RUG_THRESHOLD
    finally:
        pool.putconn(conn)

def check_quality_filters(mint):
    """
    Aplica filtros de calidad en una sola query DB.
    Retorna (bloqueado: bool, motivo: str).

    Filtros (en orden de prioridad):
      1. ONLY_0_HOLDERS en rug_flags  → hard block
      2. liquidity_usd < MIN_LIQUIDITY_USD → block
      3. Sin ninguna red social       → block
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT rug_flags, liquidity_usd, pair_address
            FROM discovered_tokens WHERE mint = %s
        """, (mint,))
        row = cur.fetchone()
        cur.close()
        if not row:
            return False, ''

        rug_flags, liquidity_usd, pair_address = row

        # 1) ONLY_0_HOLDERS — hard block
        if rug_flags and 'ONLY_0_HOLDERS' in rug_flags:
            return True, 'ONLY_0_HOLDERS (sin holders on-chain)'

        # 2) Liquidez mínima — solo aplica a tokens con par DexScreener
        # Tokens en bonding curve (sin pair_address) no tienen liquidity_usd
        if pair_address is not None:
            liq = float(liquidity_usd) if liquidity_usd is not None else 0.0
            if liq < MIN_LIQUIDITY_USD:
                return True, f'liquidez insuficiente (${liq:,.0f} < ${MIN_LIQUIDITY_USD:,})'

        return False, ''
    finally:
        pool.putconn(conn)

def compute_entry_signal(current, history):
    """Señal de momentum con detección de pump burst (mejora 12)."""
    if not history or len(history) < 2:
        return 'WAIT', 0

    prices = [float(r[0]) for r in history if r[0]]
    buys   = [r[3] for r in history if r[3] is not None]
    sells  = [r[4] for r in history if r[4] is not None]

    if len(prices) < 2:
        return 'WAIT', 0

    price_up           = prices[0] > prices[1]
    price_acceleration = (prices[0] - prices[1]) / prices[1] * 100 if prices[1] > 0 else 0
    buy_pressure       = buys[0] > sells[0] if buys and sells else False
    buy_ratio          = buys[0] / (buys[0] + sells[0]) if buys and (buys[0] + sells[0]) > 0 else 0

    # ── PUMP BURST DETECTOR ────────────────────────────────────
    # Cuenta cuántos snapshots consecutivos tienen precio subiendo.
    # 3+ consecutivos = señal de pump coordinado.
    consecutive_up = 0
    for i in range(len(prices) - 1):
        if prices[i] > prices[i + 1]:
            consecutive_up += 1
        else:
            break

    momentum = 0
    if price_up:                  momentum += 30
    if price_acceleration > 5:    momentum += 20
    elif price_acceleration > 2:  momentum += 10
    if buy_pressure:              momentum += 25
    if buy_ratio > 0.7:           momentum += 25
    elif buy_ratio > 0.5:         momentum += 10

    # Pump burst bonus
    if consecutive_up >= 4:       momentum += 30   # fuerte: 4+ snapshots subiendo
    elif consecutive_up >= 3:     momentum += 15   # moderado: 3 consecutivos

    if momentum >= 90:     return 'ENTER', momentum
    elif momentum >= 40:   return 'WATCH', momentum
    elif not price_up and not buy_pressure: return 'EXIT', momentum
    else:                  return 'WAIT', momentum

def update_entry_signal(mint, signal, price, ml_prob=None):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        if signal == 'ENTER':
            cur.execute("""
                UPDATE discovered_tokens
                SET entry_signal = %s, entry_price = %s, entry_at = NOW(),
                    ml_probability = %s
                WHERE mint = %s AND entry_signal != 'ENTER'
            """, (signal, price, ml_prob, mint))
        else:
            cur.execute("""
                UPDATE discovered_tokens
                SET entry_signal = %s, ml_probability = %s WHERE mint = %s
            """, (signal, ml_prob, mint))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_watchlist():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, name, symbol, survival_score, price_usd, market_cap
            FROM discovered_tokens
            WHERE signal IN ('STRONG_BUY', 'BUY')
            AND created_at > NOW() - INTERVAL '3 hours'
            AND price_usd IS NOT NULL
            ORDER BY survival_score DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

async def monitor_cycle():
    watchlist = get_watchlist()
    if not watchlist:
        log.info("Sin tokens en watchlist")
        return

    log.info(f"📡 Monitoreando {len(watchlist)} tokens...")
    enters = 0

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(5)

        async def process_token(token):
            nonlocal enters
            mint, name, symbol, score, orig_price, orig_mcap = token
            async with semaphore:
                data = await fetch_current_data(session, mint)
                if not data or not data["price_usd"]:
                    return

                save_snapshot(mint, data)
                history      = get_price_history(mint)
                signal, momentum = compute_entry_signal(data, history)

                # ── FILTRO RUG ────────────────────────────────
                if signal == 'ENTER' and not is_rug_safe(mint):
                    log.info(f"🛡️  RUG BLOQUEADO | {name or mint[:8]} | rug_score alto")
                    log_missed_trade(mint, name, symbol, None, 'ENTRY_SIGNAL',
                                     'RUG_FILTER', f'rug_score >= {RUG_THRESHOLD}',
                                     data["price_usd"])
                    signal = 'WAIT'

                # ── FILTROS DE CALIDAD ────────────────────────
                if signal == 'ENTER':
                    blocked, reason = check_quality_filters(mint)
                    if blocked:
                        log.info(f"🚫 CALIDAD BLOQUEADO | {name or mint[:8]} | {reason}")
                        log_missed_trade(mint, name, symbol, None, 'ENTRY_SIGNAL',
                                         'QUALITY_FILTER', reason, data["price_usd"])
                        signal = 'WAIT'

                # ── FILTRO ML ─────────────────────────────────
                ml_prob, ml_ok = ml_predict(mint)

                if signal == 'ENTER' and not ml_ok:
                    log.info(
                        f"🤖 ML BLOQUEADO | {name or mint[:8]} | "
                        f"prob={ml_prob}% < {ML_THRESHOLD*100:.0f}%"
                    )
                    log_missed_trade(mint, name, symbol, ml_prob, 'ENTRY_SIGNAL',
                                     'ML_FILTER',
                                     f'ml={ml_prob}% < {ML_THRESHOLD*100:.0f}%',
                                     data["price_usd"])
                    signal = 'WATCH'

                update_entry_signal(mint, signal, data["price_usd"], ml_prob)

                curr_price = float(data["price_usd"])
                orig       = float(orig_price) if orig_price else curr_price
                change_pct = ((curr_price - orig) / orig * 100) if orig > 0 else 0

                if signal == 'ENTER':
                    enters += 1
                    capture_features_at_enter(mint)
                    log.info(
                        f"🚨 ENTER | {name or mint[:8]} ({symbol}) | "
                        f"Score:{score} | Momentum:{momentum} | "
                        f"ML:{ml_prob}% | "
                        f"Price:${curr_price:.8f} | Change:{change_pct:+.1f}% | "
                        f"B/S:{data['buys_5m']}/{data['sells_5m']}"
                    )
                elif signal == 'EXIT':
                    log.info(
                        f"🔴 EXIT | {name or mint[:8]} | Change:{change_pct:+.1f}%"
                    )

        await asyncio.gather(*[process_token(t) for t in watchlist])

    if enters > 0:
        log.info(f"✅ {enters} señales ENTER generadas este ciclo")

async def main():
    setup_db()
    log.info("🎯 Entry Signal Engine arrancando (ML + Rug integrado)...")

    while True:
        try:
            await monitor_cycle()
            log.info("⏳ Esperando 60 segundos...")
            await asyncio.sleep(60)
        except Exception as e:
            log.error(f"Error en ciclo: {e}")
            await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
