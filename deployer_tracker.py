"""
deployer_tracker.py
-------------------
Rastrea el historial de deployers para detectar ruggers seriales.

Lógica:
  - Para cada token con deployer_wallet conocido, agrega cuántos tokens
    lanzó ese deployer y cuántos fueron rugs (rug_score > 70).
  - Marca is_serial_rugger = TRUE si rug_rate >= 50% Y total >= 3 tokens.
  - Enriquece tokens sin deployer_wallet llamando a Helius getAsset.

Timer: cada 30 minutos vía systemd.
"""

import psycopg2
import psycopg2.pool
import requests
import os
import logging
import time
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/deployer_tracker.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

RPC_URL = os.getenv("RPC_URL")

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

# ── CONFIGURACIÓN ──────────────────────────────────────
RUG_THRESHOLD     = 70    # rug_score > 70 → token considerado rug
SERIAL_MIN_TOKENS = 3     # mínimo tokens para calificar como serial rugger
SERIAL_MIN_RATE   = 0.50  # mínimo rug_rate para ser serial rugger (50%)
SUSPICIOUS_RATE   = 0.30  # rug_rate medio → sospechoso
FETCH_BATCH       = 600   # tokens a enriquecer por ciclo

# RPC pública de Solana — NO usa créditos de Helius
# Fallback si la primaria falla (rate limit 429)
PUBLIC_RPCS = [
    "https://api.mainnet-beta.solana.com",
    os.getenv("PUBLIC_RPC_URL", "https://api.mainnet-beta.solana.com"),
]


def fetch_deployer_from_tx(tx_signature):
    """
    Obtiene el deployer (fee payer = account[0]) de una transacción.
    Usa la RPC pública de Solana — no consume créditos de Helius.
    Itera sobre múltiples RPCs públicas si hay rate limit (429).
    """
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTransaction",
        "params": [tx_signature, {
            "commitment": "finalized",
            "maxSupportedTransactionVersion": 0
        }]
    }
    for rpc in PUBLIC_RPCS:
        try:
            resp = requests.post(rpc, json=payload, timeout=15).json()
            if "error" in resp:
                code = resp["error"].get("code", 0)
                if code == 429:
                    time.sleep(3)  # esperar antes de reintentar
                    continue       # rate limit → probar siguiente RPC
                log.debug(f"RPC error {rpc[:30]}: {resp['error']}")
                continue
            result = resp.get("result")
            if not result:
                return None   # tx aún no finalizada
            msg = result.get("transaction", {}).get("message", {})
            # Legacy tx: accountKeys es lista de strings
            keys = msg.get("accountKeys")
            if keys and isinstance(keys[0], str):
                return keys[0]
            # Versioned tx (v0): staticAccountKeys
            keys = msg.get("staticAccountKeys")
            if keys:
                return keys[0]
        except Exception as e:
            log.debug(f"TX lookup error ({rpc[:25]}): {e}")
            continue
    return None


def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS deployer_stats (
                wallet             TEXT PRIMARY KEY,
                total_tokens       INTEGER DEFAULT 0,
                rugged_count       INTEGER DEFAULT 0,
                avg_rug_score      NUMERIC(5,1),
                avg_survival_score NUMERIC(5,1),
                rug_rate           NUMERIC(5,3) DEFAULT 0,
                is_serial_rugger   BOOLEAN DEFAULT FALSE,
                last_token_at      TIMESTAMP,
                first_seen         TIMESTAMP DEFAULT NOW(),
                last_updated       TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        # Añadir columna deployer_wallet a discovered_tokens
        try:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("""
                ALTER TABLE discovered_tokens
                ADD COLUMN IF NOT EXISTS deployer_wallet TEXT DEFAULT NULL
            """)
            cur.execute("""
                ALTER TABLE discovered_tokens
                ADD COLUMN IF NOT EXISTS tx_signature TEXT DEFAULT NULL
            """)
            conn.commit()
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dt_deployer
                ON discovered_tokens(deployer_wallet)
                WHERE deployer_wallet IS NOT NULL
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.debug(f"ALTER/INDEX skipped (ya existe): {e}")
        cur.close()
        log.info("✅ DB deployer_tracker inicializado")
    finally:
        pool.putconn(conn)


def fetch_deployer_from_helius(mint):
    """
    Obtiene el wallet del deployer vía Helius getAsset.
    Estrategia:
      1. creators[] (Metaplex metadata — más fiable para Pump.fun)
      2. authorities[] con scope "full" (mint authority al crear)
    """
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getAsset",
        "params": {
            "id": mint,
            "displayOptions": {"showSystemMetadata": True, "showFungible": True}
        }
    }
    try:
        resp = requests.post(RPC_URL, json=payload, timeout=10).json()
        result = resp.get("result") or {}

        # Método 1: creators array (Metaplex)
        for creator in result.get("creators", []):
            addr = creator.get("address", "")
            # Excluir programas conocidos (solo wallets reales)
            if addr and len(addr) > 30 and not addr.startswith("11111111"):
                return addr

        # Método 2: authorities con scope "full"
        for auth in result.get("authorities", []):
            if "full" in auth.get("scopes", []):
                addr = auth.get("address", "")
                if addr and len(addr) > 30 and not addr.startswith("11111111"):
                    return addr

    except Exception as e:
        log.debug(f"Helius error {mint[:8]}: {e}")
    return None


def enrich_deployer_wallets():
    """
    Busca deployer_wallet para tokens sin él usando la tx_signature guardada.
    Usa la RPC pública de Solana (no consume créditos de Helius).
    Prioriza tokens con rug_score calculado y tokens recientes con tx_signature.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, tx_signature FROM discovered_tokens
            WHERE deployer_wallet IS NULL
              AND tx_signature IS NOT NULL
              AND (
                rug_checked_at IS NOT NULL
                OR created_at > NOW() - INTERVAL '6 hours'
              )
            ORDER BY
                CASE WHEN rug_checked_at IS NOT NULL THEN 0 ELSE 1 END,
                created_at DESC
            LIMIT %s
        """, (FETCH_BATCH,))
        rows = cur.fetchall()
        cur.close()
    finally:
        pool.putconn(conn)

    if not rows:
        log.info("No hay tokens con tx_signature pendientes")
        return 0

    log.info(f"🔍 Buscando deployer para {len(rows)} tokens (RPC pública)...")
    found = 0
    for mint, tx_sig in rows:
        deployer = fetch_deployer_from_tx(tx_sig)
        if deployer:
            conn = pool.getconn()
            try:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE discovered_tokens SET deployer_wallet = %s WHERE mint = %s",
                    (deployer, mint)
                )
                conn.commit()
                cur.close()
                found += 1
                log.debug(f"  {mint[:12]}... → deployer: {deployer[:12]}...")
            finally:
                pool.putconn(conn)
        time.sleep(1.2)   # RPC pública: máx ~1 req/s para evitar 429

    log.info(f"✅ Deployers encontrados: {found}/{len(rows)}")
    return found


def compute_deployer_stats():
    """
    Agrega estadísticas por deployer y detecta ruggers seriales.
    Solo considera tokens donde ya tenemos rug_score calculado.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                deployer_wallet,
                COUNT(*)                                               as total_tokens,
                COUNT(CASE WHEN rug_score > %s THEN 1 END)            as rugged_count,
                ROUND(AVG(rug_score)::numeric, 1)                     as avg_rug_score,
                ROUND(AVG(survival_score)::numeric, 1)                as avg_survival_score,
                MAX(created_at)                                        as last_token_at
            FROM discovered_tokens
            WHERE deployer_wallet IS NOT NULL
              AND rug_score IS NOT NULL
            GROUP BY deployer_wallet
        """, (RUG_THRESHOLD,))
        rows = cur.fetchall()
        cur.close()
    finally:
        pool.putconn(conn)

    if not rows:
        log.info("Sin datos suficientes de deployers aún")
        return 0

    conn = pool.getconn()
    serial_count = 0
    suspicious_count = 0
    try:
        cur = conn.cursor()
        for wallet, total, rugged, avg_rug, avg_surv, last_at in rows:
            rug_rate   = rugged / total if total > 0 else 0
            is_serial  = (total >= SERIAL_MIN_TOKENS and rug_rate >= SERIAL_MIN_RATE)
            if is_serial:
                serial_count += 1
            elif rug_rate >= SUSPICIOUS_RATE and total >= 2:
                suspicious_count += 1

            cur.execute("""
                INSERT INTO deployer_stats
                    (wallet, total_tokens, rugged_count, avg_rug_score,
                     avg_survival_score, rug_rate, is_serial_rugger,
                     last_token_at, last_updated)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (wallet) DO UPDATE SET
                    total_tokens       = EXCLUDED.total_tokens,
                    rugged_count       = EXCLUDED.rugged_count,
                    avg_rug_score      = EXCLUDED.avg_rug_score,
                    avg_survival_score = EXCLUDED.avg_survival_score,
                    rug_rate           = EXCLUDED.rug_rate,
                    is_serial_rugger   = EXCLUDED.is_serial_rugger,
                    last_token_at      = EXCLUDED.last_token_at,
                    last_updated       = NOW()
            """, (wallet, total, rugged, avg_rug, avg_surv,
                  round(rug_rate, 3), is_serial, last_at))

        conn.commit()
        cur.close()
        log.info(f"📊 {len(rows)} deployers procesados | "
                 f"Serial ruggers: {serial_count} | Sospechosos: {suspicious_count}")

        # Mostrar top ruggers
        if serial_count > 0:
            conn2 = pool.getconn()
            try:
                cur2 = conn2.cursor()
                cur2.execute("""
                    SELECT wallet, total_tokens, rugged_count, rug_rate
                    FROM deployer_stats
                    WHERE is_serial_rugger = TRUE
                    ORDER BY rug_rate DESC, rugged_count DESC
                    LIMIT 5
                """)
                for w, t, r, rate in cur2.fetchall():
                    log.warning(f"🚨 RUGGER: {w[:16]}... | {r}/{t} rugs | rate={rate:.0%}")
                cur2.close()
            finally:
                pool.putconn(conn2)

    finally:
        pool.putconn(conn)
    return serial_count


def get_summary():
    """Imprime resumen de estado del deployer tracker."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*)                                    as total_tokens,
                COUNT(deployer_wallet)                      as with_deployer,
                COUNT(DISTINCT deployer_wallet)             as unique_deployers
            FROM discovered_tokens
        """)
        t, d, u = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM deployer_stats WHERE is_serial_rugger = TRUE")
        serial = cur.fetchone()[0]
        cur.close()
        coverage = d / t * 100 if t > 0 else 0
        log.info(f"📈 Estado: {d}/{t} tokens con deployer ({coverage:.1f}%) | "
                 f"{u} deployers únicos | {serial} serial ruggers en lista negra")
    finally:
        pool.putconn(conn)


def run():
    setup_db()
    log.info("🕵️ Deployer Tracker iniciando...")
    enrich_deployer_wallets()
    compute_deployer_stats()
    get_summary()
    log.info("✅ Deployer Tracker completado")


if __name__ == "__main__":
    run()
