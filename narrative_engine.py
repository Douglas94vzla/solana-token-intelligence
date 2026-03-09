import psycopg2
import psycopg2.pool
import os
import re
import logging
from datetime import datetime
from collections import Counter
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/narrative_engine.log'),
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

# ── NARRATIVAS CONOCIDAS ──────────────────────────────
NARRATIVE_PATTERNS = {
    "AI/AGI":        ["ai", "agi", "gpt", "neural", "intelligence", "agent", "robot", "cyber", "neuro", "brain", "llm", "claude", "openai"],
    "OIL/ENERGY":    ["oil", "petroleum", "crude", "petro", "energy", "gas", "barrel", "opec", "fuel"],
    "MEME/DOG":      ["dog", "inu", "doge", "shib", "cat", "pepe", "wojak", "frog", "moon", "bonk"],
    "POLITICS":      ["trump", "biden", "maga", "elon", "president", "white house", "congress", "senate", "election"],
    "ANIME/JAPAN":   ["anime", "waifu", "kawaii", "japan", "tokyo", "ninja", "samurai", "manga"],
    "DEFI/CRYPTO":   ["defi", "yield", "stake", "liquidity", "protocol", "dao", "vault", "swap"],
    "SPORTS":        ["nfl", "nba", "fifa", "football", "basketball", "soccer", "baseball", "championship"],
    "CHINA/ASIA":    ["china", "chinese", "asia", "hong kong", "taiwan", "dragon", "panda"],
    "MILITARY":      ["military", "war", "army", "navy", "missile", "weapon", "fighter", "bomb"],
    "SPACE":         ["space", "nasa", "moon", "mars", "rocket", "satellite", "galaxy", "star", "asteroid"],
    "FOOD":          ["food", "burger", "pizza", "taco", "sushi", "coffee", "beer", "wine"],
    "ABSURD/NIL":    ["nothing", "shitcoin", "retard", "stupid", "dumb", "useless", "worthless", "trash", "garbage", "shit", "fuck"],
    "WEALTH":        ["trillion", "billion", "millionaire", "trillionaire", "rich", "wealth", "lambo", "yacht", "luxury"],
    "BLACK SWAN":    ["black swan", "blackswan", "chaos", "crash", "collapse", "crisis", "recession", "bear"],
    "SPORTS/SOCCER": ["camavinga", "mbappe", "ronaldo", "messi", "neymar", "soccer", "premier", "laliga", "champions"],
    "IDENTITY":      ["she", "he", "they", "real", "life", "irl", "you", "me", "us", "human", "person"],
    "HEALTH/BIO":    ["health", "bio", "pharma", "vaccine", "virus", "medical", "dna", "gene"],
}

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS narrative_stats (
                id SERIAL PRIMARY KEY,
                narrative TEXT NOT NULL,
                token_count INTEGER DEFAULT 0,
                avg_volume NUMERIC(20,2) DEFAULT 0,
                total_volume NUMERIC(20,2) DEFAULT 0,
                avg_score NUMERIC(5,2) DEFAULT 0,
                top_token TEXT,
                top_token_volume NUMERIC(20,2) DEFAULT 0,
                momentum NUMERIC(10,2) DEFAULT 0,
                window_hours INTEGER DEFAULT 24,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_narrative_stats_time 
                ON narrative_stats(calculated_at DESC);
            
            ALTER TABLE discovered_tokens
            ADD COLUMN IF NOT EXISTS narrative TEXT DEFAULT NULL;
        """)
        conn.commit()
        cur.close()
        log.info("✅ Tablas de narrativas creadas")
    finally:
        pool.putconn(conn)

def classify_token_narrative(name, symbol, description):
    """Clasifica un token en una narrativa basada en su nombre/descripción"""
    text = " ".join(filter(None, [name, symbol, description])).lower()
    
    scores = {}
    for narrative, keywords in NARRATIVE_PATTERNS.items():
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            scores[narrative] = score
    
    if not scores:
        return "OTHER"
    
    return max(scores, key=scores.get)

def tag_tokens_with_narratives():
    """Clasifica todos los tokens sin narrativa"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, name, symbol, description
            FROM discovered_tokens
            WHERE narrative IS NULL
            AND created_at > NOW() - INTERVAL '7 days'
            LIMIT 5000
        """)
        tokens = cur.fetchall()
        
        updates = []
        for mint, name, symbol, desc in tokens:
            narrative = classify_token_narrative(name, symbol, desc)
            updates.append((narrative, mint))
        
        if updates:
            cur.executemany(
                "UPDATE discovered_tokens SET narrative = %s WHERE mint = %s",
                updates
            )
            conn.commit()
            log.info(f"✅ {len(updates)} tokens clasificados")
        
        cur.close()
    finally:
        pool.putconn(conn)

def calculate_narrative_stats(window_hours=24):
    """Calcula estadísticas por narrativa para una ventana de tiempo"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                narrative,
                COUNT(*) as token_count,
                ROUND(AVG(volume_24h)::numeric, 2) as avg_volume,
                ROUND(SUM(volume_24h)::numeric, 2) as total_volume,
                ROUND(AVG(survival_score)::numeric, 2) as avg_score,
                MAX(name) as top_name,
                MAX(volume_24h) as top_volume
            FROM discovered_tokens
            WHERE created_at > NOW() - INTERVAL '%s hours'
            AND volume_24h > 0
            AND narrative IS NOT NULL
            GROUP BY narrative
            ORDER BY total_volume DESC
        """ % window_hours)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def calculate_momentum(narrative, window_hours=6):
    """
    Momentum = crecimiento de volumen en últimas 6h vs 6h anteriores
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COALESCE(SUM(CASE WHEN created_at > NOW() - INTERVAL '%s hours' 
                    THEN volume_24h END), 0) as recent_vol,
                COALESCE(SUM(CASE WHEN created_at BETWEEN NOW() - INTERVAL '%s hours' 
                    AND NOW() - INTERVAL '%s hours' 
                    THEN volume_24h END), 0) as prev_vol
            FROM discovered_tokens
            WHERE narrative = %%s
            AND volume_24h > 0
        """ % (window_hours, window_hours*2, window_hours), (narrative,))
        row = cur.fetchone()
        cur.close()
        
        if not row or not row[1] or row[1] == 0:
            return 0
        
        recent, prev = float(row[0]), float(row[1])
        if prev == 0:
            return 100 if recent > 0 else 0
        
        return round((recent - prev) / prev * 100, 1)
    finally:
        pool.putconn(conn)

def save_narrative_stats(stats, window_hours):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        for narrative, count, avg_vol, total_vol, avg_score, top_name, top_vol in stats:
            momentum = calculate_momentum(narrative)
            cur.execute("""
                INSERT INTO narrative_stats 
                (narrative, token_count, avg_volume, total_volume, avg_score, 
                 top_token, top_token_volume, momentum, window_hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (narrative, count, avg_vol, total_vol, avg_score,
                  top_name, top_vol, momentum, window_hours))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def print_narrative_report(stats, window_hours):
    print("\n" + "="*70)
    print(f"📡 NARRATIVE INTELLIGENCE — Últimas {window_hours}h — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*70)
    
    for narrative, count, avg_vol, total_vol, avg_score, top_name, top_vol in stats[:15]:
        momentum = calculate_momentum(narrative)
        
        if momentum > 50:
            trend = "🚀 PUMPING"
        elif momentum > 20:
            trend = "📈 RISING"
        elif momentum > 0:
            trend = "➡️  STABLE"
        elif momentum < -20:
            trend = "📉 FADING"
        else:
            trend = "⬇️  DYING"
        
        print(f"\n  {trend} | {narrative:<15}")
        print(f"    Tokens:      {count}")
        print(f"    Vol Total:   ${total_vol:>12,.0f}")
        print(f"    Vol Avg:     ${avg_vol:>12,.0f}")
        print(f"    Momentum:    {momentum:+.1f}%")
        print(f"    Score Avg:   {avg_score or 0:.1f}")
        print(f"    Top Token:   {top_name or 'Unknown'}")
    
    print("\n" + "="*70)
    
    # Top 3 narrativas emergentes
    print("\n🔥 TOP 3 NARRATIVAS EMERGENTES AHORA:")
    emerging = sorted(
        [(n, c, av, tv, sc, tn, tv2) for n, c, av, tv, sc, tn, tv2 in stats if c >= 2],
        key=lambda x: calculate_momentum(x[0]),
        reverse=True
    )[:3]
    
    for i, (narrative, count, avg_vol, total_vol, avg_score, top_name, top_vol) in enumerate(emerging, 1):
        momentum = calculate_momentum(narrative)
        print(f"  #{i} {narrative} — {count} tokens — Vol: ${total_vol:,.0f} — Momentum: {momentum:+.1f}%")
    
    print("="*70 + "\n")

def run():
    setup_db()
    log.info("📡 Narrative Engine iniciando...")
    
    log.info("🏷️  Clasificando tokens...")
    tag_tokens_with_narratives()
    
    for window in [6, 24]:
        log.info(f"📊 Calculando stats para ventana {window}h...")
        stats = calculate_narrative_stats(window)
        save_narrative_stats(stats, window)
        
        if window == 24:
            print_narrative_report(stats, window)
    
    log.info("✅ Narrative Engine completado")

if __name__ == "__main__":
    run()
