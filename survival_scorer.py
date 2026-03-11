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
        cur.close()
        log.info("✅ Columnas de scoring añadidas")
    finally:
        pool.putconn(conn)

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
