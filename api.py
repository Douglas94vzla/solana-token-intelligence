from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.pool
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Solana Token Intelligence API",
    description="Real-time token trend analysis on Solana / Pump.fun",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 10,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

def get_conn():
    return pool.getconn()

def release_conn(conn):
    pool.putconn(conn)

# ─────────────────────────────────────────
# ROOT
# ─────────────────────────────────────────
@app.get("/")
def root():
    return {
        "project": "Solana Token Intelligence Platform",
        "version": "1.0.0",
        "status": "online",
        "endpoints": ["/health", "/tokens", "/trends", "/alerts", "/stats"]
    }

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────
@app.get("/health")
def health():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM discovered_tokens")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM discovered_tokens WHERE created_at > NOW() - INTERVAL '1 hour'")
        last_hour = cur.fetchone()[0]
        cur.close()
        return {
            "status": "healthy",
            "database": "connected",
            "total_tokens": total,
            "tokens_last_hour": last_hour,
            "timestamp": datetime.utcnow().isoformat()
        }
    finally:
        release_conn(conn)

# ─────────────────────────────────────────
# TOKENS
# ─────────────────────────────────────────
@app.get("/tokens")
def get_tokens(
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    hours: int = Query(default=24),
    with_name: bool = Query(default=True)
):
    conn = get_conn()
    try:
        cur = conn.cursor()
        since = datetime.utcnow() - timedelta(hours=hours)
        name_filter = "AND name IS NOT NULL AND name != ''" if with_name else ""
        cur.execute(f"""
            SELECT mint, name, symbol, status, score, created_at,
                   twitter, telegram, website
            FROM discovered_tokens
            WHERE created_at > %s {name_filter}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, (since, limit, offset))
        rows = cur.fetchall()
        cur.close()
        return {
            "total": len(rows),
            "hours": hours,
            "tokens": [
                {
                    "mint": r[0],
                    "name": r[1],
                    "symbol": r[2],
                    "status": r[3],
                    "score": r[4],
                    "created_at": r[5].isoformat() if r[5] else None,
                    "twitter": r[6],
                    "telegram": r[7],
                    "website": r[8]
                }
                for r in rows
            ]
        }
    finally:
        release_conn(conn)

# ─────────────────────────────────────────
# TRENDS
# ─────────────────────────────────────────
@app.get("/trends")
def get_trends(hours: int = Query(default=1)):
    conn = get_conn()
    try:
        cur = conn.cursor()
        since = datetime.utcnow() - timedelta(hours=hours)
        cur.execute("""
            SELECT keyword, COUNT(*) as count
            FROM keyword_index
            WHERE created_at > %s
            GROUP BY keyword
            ORDER BY count DESC
            LIMIT 30
        """, (since,))
        rows = cur.fetchall()
        cur.close()
        return {
            "hours": hours,
            "since": since.isoformat(),
            "trends": [{"keyword": r[0], "count": r[1]} for r in rows]
        }
    finally:
        release_conn(conn)

# ─────────────────────────────────────────
# EMERGING NARRATIVES
# ─────────────────────────────────────────
@app.get("/trends/emerging")
def get_emerging():
    conn = get_conn()
    try:
        cur = conn.cursor()
        now_bucket = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
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
            AND curr.count >= 3
            ORDER BY growth_pct DESC
            LIMIT 20
        """, (prev_bucket, now_bucket))
        rows = cur.fetchall()
        cur.close()
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "emerging": [
                {
                    "keyword": r[0],
                    "current_count": r[1],
                    "prev_count": r[2],
                    "growth_pct": round(r[3], 1)
                }
                for r in rows
            ]
        }
    finally:
        release_conn(conn)

# ─────────────────────────────────────────
# STATS
# ─────────────────────────────────────────
@app.get("/stats")
def get_stats():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM discovered_tokens")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM discovered_tokens WHERE name IS NOT NULL AND name != ''")
        with_name = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM discovered_tokens WHERE created_at > NOW() - INTERVAL '1 hour'")
        last_hour = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM discovered_tokens WHERE created_at > NOW() - INTERVAL '24 hours'")
        last_day = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM keyword_index")
        total_keywords = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT keyword) FROM keyword_index")
        unique_keywords = cur.fetchone()[0]
        cur.close()
        return {
            "tokens": {
                "total": total,
                "with_metadata": with_name,
                "metadata_pct": round(with_name / total * 100, 1) if total > 0 else 0,
                "last_hour": last_hour,
                "last_24h": last_day
            },
            "keywords": {
                "total_indexed": total_keywords,
                "unique": unique_keywords
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    finally:
        release_conn(conn)

# ─────────────────────────────────────────
# SEARCH
# ─────────────────────────────────────────
@app.get("/tokens/search")
def search_tokens(q: str = Query(..., min_length=2)):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, name, symbol, score, created_at
            FROM discovered_tokens
            WHERE name ILIKE %s OR symbol ILIKE %s
            ORDER BY created_at DESC
            LIMIT 50
        """, (f"%{q}%", f"%{q}%"))
        rows = cur.fetchall()
        cur.close()
        return {
            "query": q,
            "results": len(rows),
            "tokens": [
                {
                    "mint": r[0],
                    "name": r[1],
                    "symbol": r[2],
                    "score": r[3],
                    "created_at": r[4].isoformat() if r[4] else None
                }
                for r in rows
            ]
        }
    finally:
        release_conn(conn)

