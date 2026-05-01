import psycopg2
import psycopg2.pool
import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/scorer.log'),
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

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        try:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("""
                ALTER TABLE discovered_tokens
                ADD COLUMN IF NOT EXISTS survival_score INTEGER DEFAULT 0,
                ADD COLUMN IF NOT EXISTS signal TEXT DEFAULT 'IGNORE',
                ADD COLUMN IF NOT EXISTS scored_at TIMESTAMP
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"ALTER TABLE skipped (lock timeout): {e}")

        # Tabla de features en el momento exacto del primer signal — sin leakage temporal
        cur.execute("""
            CREATE TABLE IF NOT EXISTS token_features_at_signal (
                id                   SERIAL PRIMARY KEY,
                mint                 TEXT NOT NULL UNIQUE,
                signal               TEXT NOT NULL,
                captured_at          TIMESTAMP DEFAULT NOW(),
                price_at_signal      NUMERIC,
                buys_5m              INT,
                sells_5m             INT,
                market_cap           NUMERIC,
                volume_24h           NUMERIC,
                volume_1h            NUMERIC DEFAULT 0,
                volume_5m            NUMERIC DEFAULT 0,
                survival_score       INT,
                narrative            TEXT,
                buys_1h              INT,
                sells_1h             INT,
                buys_24h             INT DEFAULT 0,
                sells_24h            INT DEFAULT 0,
                rug_score            INT DEFAULT 50,
                holder_count         INT DEFAULT 10,
                top10_concentration  NUMERIC DEFAULT 95,
                dev_sold             BOOLEAN DEFAULT FALSE,
                has_twitter          BOOLEAN DEFAULT FALSE,
                has_telegram         BOOLEAN DEFAULT FALSE,
                has_website          BOOLEAN DEFAULT FALSE,
                launch_hour          INT,
                liquidity_usd        NUMERIC DEFAULT 0,
                fdv                  NUMERIC DEFAULT 0,
                smart_money_bought   INT DEFAULT 0,
                narrative_momentum   NUMERIC DEFAULT 0,
                deployer_prior_tokens INT DEFAULT 0,
                deployer_rugged_count INT DEFAULT 0,
                deployer_rug_rate    NUMERIC DEFAULT 0.5,
                is_known_rugger      INT DEFAULT 0,
                deployer_known       INT DEFAULT 0
            )
        """)
        conn.commit()
        cur.close()
        log.info("✅ Columnas de scoring y tabla token_features_at_signal listas")
    finally:
        pool.putconn(conn)


def capture_features_at_signal(cur, mint, signal):
    """
    Guarda una foto de todas las features del token en el momento del primer signal BUY/STRONG_BUY.
    ON CONFLICT DO NOTHING → solo captura la primera vez.
    """
    try:
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
                dt.mint, %s, NOW(), dt.price_usd,
                dt.buys_5m, dt.sells_5m, dt.market_cap, dt.volume_24h,
                COALESCE(dt.volume_1h, 0), COALESCE(dt.volume_5m, 0),
                dt.survival_score, dt.narrative, dt.buys_1h, dt.sells_1h,
                COALESCE(dt.buys_24h, 0), COALESCE(dt.sells_24h, 0),
                COALESCE(dt.rug_score, 50), COALESCE(dt.holder_count, 10),
                COALESCE(dt.top10_concentration, 95), COALESCE(dt.dev_sold, FALSE),
                (dt.twitter  IS NOT NULL), (dt.telegram IS NOT NULL), (dt.website IS NOT NULL),
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
            ON CONFLICT (mint) DO NOTHING
        """, (signal, mint))
    except Exception as e:
        log.warning(f"capture_features_at_signal falló para {mint[:8]}: {e}")

def compute_score(token):
    """
    Scoring de supervivencia 0-100 basado en datos de los primeros 5 minutos.
    
    Factores:
    - buys_5m:     compras en primeros 5 min (señal más fuerte)
    - sells_5m:    ventas en primeros 5 min (presión bajista)
    - buy_sell_ratio: ratio compras/ventas (momentum)
    - market_cap:  capitalización inicial
    - volume_24h:  volumen acumulado
    - has_name:    tiene metadata (señal de proyecto real)
    """
    score = 0
    signal = 'IGNORE'

    buys = token.get('buys_5m') or 0
    sells = token.get('sells_5m') or 0
    mcap = float(token.get('market_cap') or 0)
    vol = float(token.get('volume_24h') or 0)
    name = token.get('name')

    # ── FACTOR 1: Buys en primeros 5 min (40 puntos máx)
    # Basado en nuestros datos: buys=1 → $178K vol, buys=12 → $3.2M vol
    if buys == 0:
        score += 0
    elif buys == 1:
        score += 15
    elif buys == 2:
        score += 22
    elif buys <= 5:
        score += 28
    elif buys <= 10:
        score += 34
    elif buys <= 20:
        score += 40
    else:
        score += 40  # cap en 40

    # ── FACTOR 2: Ratio compras/ventas (20 puntos máx)
    if buys > 0:
        total_txns = buys + sells
        buy_ratio = buys / total_txns if total_txns > 0 else 1.0
        if buy_ratio >= 0.8:
            score += 20   # 80%+ compras — muy bullish
        elif buy_ratio >= 0.6:
            score += 14
        elif buy_ratio >= 0.5:
            score += 8
        else:
            score += 0    # más ventas que compras — peligro

    # ── FACTOR 3: Market Cap inicial (20 puntos máx)
    # Zona óptima: $1K - $50K (suficiente liquidez, room to grow)
    if 1000 <= mcap <= 10000:
        score += 20
    elif 10000 < mcap <= 50000:
        score += 15
    elif 50000 < mcap <= 100000:
        score += 10
    elif mcap > 100000:
        score += 5    # ya subió mucho, menos upside
    else:
        score += 0    # muy bajo, sospechoso

    # ── FACTOR 4: Tiene metadata (10 puntos)
    if name and len(name) > 0:
        score += 10

    # ── FACTOR 5: Volumen acumulado (10 puntos)
    if vol >= 10000:
        score += 10
    elif vol >= 1000:
        score += 6
    elif vol >= 100:
        score += 3

    # ── SEÑAL FINAL basada en score
    if score >= 70:
        signal = 'STRONG_BUY'
    elif score >= 50:
        signal = 'BUY'
    elif score >= 30:
        signal = 'WATCH'
    else:
        signal = 'IGNORE'

    return score, signal

def score_recent_tokens():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, name, symbol, buys_5m, sells_5m,
                   market_cap, volume_24h, price_usd, created_at
            FROM discovered_tokens
            WHERE price_usd IS NOT NULL
            AND (scored_at IS NULL OR scored_at < NOW() - INTERVAL '10 minutes')
            AND created_at > NOW() - INTERVAL '24 hours'
            ORDER BY created_at DESC
            LIMIT 200
        """)
        tokens = cur.fetchall()
        cols = ['mint','name','symbol','buys_5m','sells_5m',
                'market_cap','volume_24h','price_usd','created_at']
        
        results = {'STRONG_BUY': 0, 'BUY': 0, 'WATCH': 0, 'IGNORE': 0}
        
        for row in tokens:
            token = dict(zip(cols, row))
            score, signal = compute_score(token)
            
            cur.execute("""
                UPDATE discovered_tokens
                SET survival_score = %s, signal = %s, scored_at = NOW()
                WHERE mint = %s
            """, (score, signal, token['mint']))

            # capture_features_at_signal disabled — capture now happens at ENTER
            # time in entry_signal.py (capture_features_at_enter) so liquidity_usd
            # and narrative are populated when DexScreener pair exists.

            results[signal] += 1

            if signal in ('STRONG_BUY', 'BUY'):
                log.info(f"🎯 {signal} | Score: {score} | {token['name']} ({token['symbol']}) | "
                        f"Buys5m: {token['buys_5m']} | MCap: ${token['market_cap']} | "
                        f"Vol24h: ${token['volume_24h']}")

        conn.commit()
        cur.close()
        
        log.info(f"📊 Scored {len(tokens)} tokens → "
                f"STRONG_BUY: {results['STRONG_BUY']} | "
                f"BUY: {results['BUY']} | "
                f"WATCH: {results['WATCH']} | "
                f"IGNORE: {results['IGNORE']}")
        
        return results

    finally:
        pool.putconn(conn)

def print_opportunities():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT name, symbol, survival_score, signal,
                   buys_5m, sells_5m, price_usd, market_cap, volume_24h,
                   created_at
            FROM discovered_tokens
            WHERE signal IN ('STRONG_BUY', 'BUY')
            AND created_at > NOW() - INTERVAL '24 hours'
            ORDER BY survival_score DESC, created_at DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()

        print("\n" + "="*70)
        print(f"🎯 OPORTUNIDADES DETECTADAS — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("="*70)
        for r in rows:
            name, symbol, score, signal, b5, s5, price, mcap, vol, created = r
            age = datetime.now() - created.replace(tzinfo=None)
            age_min = int(age.total_seconds() / 60)
            print(f"\n  {'🔥' if signal == 'STRONG_BUY' else '✅'} [{signal}] Score: {score}/100")
            print(f"     Token:   {name} ({symbol})")
            print(f"     Precio:  ${float(price):.8f}")
            print(f"     MCap:    ${float(mcap):,.0f}")
            print(f"     Vol24h:  ${float(vol):,.0f}")
            print(f"     Buys5m:  {b5} | Sells5m: {s5}")
            print(f"     Edad:    {age_min} minutos")
        print("="*70 + "\n")

    finally:
        pool.putconn(conn)

if __name__ == "__main__":
    setup_db()
    log.info("🧠 Survival Scorer iniciando...")
    score_recent_tokens()
    print_opportunities()
    log.info("✅ Scoring completado")
