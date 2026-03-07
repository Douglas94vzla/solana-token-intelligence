import psycopg2
import psycopg2.pool
import os
import re
import logging
from collections import Counter
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Logging profesional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/trend_engine.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# Palabras vacías a ignorar
STOPWORDS = {
    'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for', 'is', 'are',
    'was', 'by', 'with', 'and', 'or', 'but', 'not', 'this', 'that', 'it',
    'its', 'be', 'as', 'my', 'your', 'our', 'i', 'me', 'we', 'you', 'he',
    'she', 'they', 'do', 'did', 'will', 'can', 'just', 'so', 'up', 'out',
    'de', 'la', 'el', 'en', 'es', 'un', 'una', 'los', 'las', 'del', 'con'
}

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

def extract_keywords(text):
    if not text:
        return []
    text = text.lower()
    # Eliminar caracteres especiales, dejar letras y números
    words = re.findall(r'[a-z0-9]+', text)
    # Filtrar stopwords y palabras muy cortas
    keywords = [w for w in words if w not in STOPWORDS and len(w) >= 3]
    return keywords

def index_keywords(hours_back=24):
    """Indexa keywords de tokens recientes"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        since = datetime.now() - timedelta(hours=hours_back)
        cur.execute("""
            SELECT mint, name, symbol FROM discovered_tokens
            WHERE name IS NOT NULL AND name != ''
            AND created_at > %s
        """, (since,))
        tokens = cur.fetchall()
        log.info(f"Indexando keywords de {len(tokens)} tokens de las últimas {hours_back}h")

        indexed = 0
        for mint, name, symbol in tokens:
            keywords = extract_keywords(name) + extract_keywords(symbol)
            for kw in set(keywords):
                try:
                    cur.execute("""
                        INSERT INTO keyword_index (mint, keyword, created_at)
                        VALUES (%s, %s, (SELECT created_at FROM discovered_tokens WHERE mint=%s))
                        ON CONFLICT DO NOTHING
                    """, (mint, kw, mint))
                    indexed += 1
                except Exception:
                    pass
        conn.commit()
        log.info(f"Keywords indexadas: {indexed}")
        cur.close()
    finally:
        pool.putconn(conn)

def compute_trends(hours_back=1):
    """Calcula tendencias del último período"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        since = datetime.now() - timedelta(hours=hours_back)
        hour_bucket = datetime.now().replace(minute=0, second=0, microsecond=0)

        cur.execute("""
            SELECT keyword, COUNT(*) as cnt
            FROM keyword_index
            WHERE created_at > %s
            GROUP BY keyword
            ORDER BY cnt DESC
            LIMIT 50
        """, (since,))
        trends = cur.fetchall()

        for keyword, count in trends:
            cur.execute("""
                INSERT INTO trend_analysis (keyword, count, hour_bucket)
                VALUES (%s, %s, %s)
                ON CONFLICT (keyword, hour_bucket)
                DO UPDATE SET count = EXCLUDED.count
            """, (keyword, count, hour_bucket))

        conn.commit()
        cur.close()
        return trends
    finally:
        pool.putconn(conn)

def detect_emerging(min_count=5):
    """Detecta narrativas emergentes comparando última hora vs hora anterior"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        now_bucket = datetime.now().replace(minute=0, second=0, microsecond=0)
        prev_bucket = now_bucket - timedelta(hours=1)

        cur.execute("""
            SELECT 
                curr.keyword,
                curr.count as current_count,
                COALESCE(prev.count, 0) as prev_count,
                CASE 
                    WHEN COALESCE(prev.count, 0) = 0 THEN curr.count * 100
                    ELSE ((curr.count - prev.count)::float / prev.count * 100)
                END as growth_pct
            FROM trend_analysis curr
            LEFT JOIN trend_analysis prev 
                ON curr.keyword = prev.keyword 
                AND prev.hour_bucket = %s
            WHERE curr.hour_bucket = %s
            AND curr.count >= %s
            ORDER BY growth_pct DESC
            LIMIT 20
        """, (prev_bucket, now_bucket, min_count))

        emerging = cur.fetchall()
        cur.close()
        return emerging
    finally:
        pool.putconn(conn)

def print_dashboard():
    """Imprime resumen de tendencias en consola"""
    trends = compute_trends(hours_back=1)
    emerging = detect_emerging(min_count=3)

    print("\n" + "="*60)
    print(f"🔥 TOP TENDENCIAS — Última hora ({datetime.now().strftime('%H:%M')})")
    print("="*60)
    for keyword, count in trends[:20]:
        bar = "█" * min(count, 30)
        print(f"  {keyword:<20} {bar} {count}")

    print("\n" + "="*60)
    print("🚀 NARRATIVAS EMERGENTES")
    print("="*60)
    for kw, curr, prev, growth in emerging[:10]:
        arrow = "🆕" if prev == 0 else "📈"
        print(f"  {arrow} {kw:<20} {curr} tokens (+{growth:.0f}%)")
    print("="*60 + "\n")

if __name__ == "__main__":
    # Crear directorio de logs
    os.makedirs('/var/log/solana_bot', exist_ok=True)
    log.info("🧠 Motor de tendencias iniciando...")
    index_keywords(hours_back=24)
    print_dashboard()
    log.info("✅ Ciclo completado")
