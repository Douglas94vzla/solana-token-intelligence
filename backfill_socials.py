"""
backfill_socials.py — Rellena twitter/telegram/website/description/image_url
para tokens que ya tienen nombre pero nunca tuvieron socials (enriquecidos
con el viejo metadata_fetcher que no extraía URIs).

Prioridad:
  1. Tokens en token_features_at_signal (impacto directo en ML)
  2. Tokens con señal BUY / STRONG_BUY
  3. Resto de tokens con nombre, más recientes primero

Corre como timer (mismo que metadata-fetcher.timer) o manualmente:
    python backfill_socials.py

Velocidad esperada: ~150-200 tokens/min (solo Token-2022 + URI fetch).
Tiempo total para 95k tokens: ~8-10 horas en batches de 200 cada 5min.
"""

import psycopg2
import os
import time
import logging
from dotenv import load_dotenv
from metadata_fetcher import fetch_token2022_uri, fetch_uri_metadata

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/backfill_socials.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

BATCH_SIZE = 200


def get_db():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"), host=os.getenv("DB_HOST")
    )


def get_batch(cur) -> list[str]:
    """
    Returns up to BATCH_SIZE mints with name but no socials yet.
    Priority order:
      1. Tokens in token_features_at_signal (ML training data)
      2. BUY / STRONG_BUY tokens
      3. Everything else, newest first
    """
    cur.execute(f"""
        SELECT dt.mint FROM discovered_tokens dt
        WHERE dt.name IS NOT NULL
          AND dt.twitter  IS NULL
          AND dt.telegram IS NULL
          AND dt.website  IS NULL
        ORDER BY
            -- Priority 1: in ML signal table
            (EXISTS (SELECT 1 FROM token_features_at_signal tfs WHERE tfs.mint = dt.mint)) DESC,
            -- Priority 2: BUY/STRONG_BUY
            (dt.signal IN ('STRONG_BUY', 'BUY')) DESC,
            -- Priority 3: newest first
            dt.created_at DESC
        LIMIT {BATCH_SIZE}
    """)
    return [r[0] for r in cur.fetchall()]


def process_token(mint: str) -> dict | None:
    """
    Returns social data if found, None if no URI or URI yields nothing.
    Does NOT touch name/symbol/deployer (those are already set).
    """
    t22 = fetch_token2022_uri(mint)
    if not t22:
        return None

    uri = t22.get("uri", "")
    if not uri:
        return None

    social = fetch_uri_metadata(uri)
    # Only return if at least one social field is present
    if any(social.get(k) for k in ("description", "image_url", "twitter", "telegram", "website")):
        return social
    return None


def save(cur, mint: str, social: dict):
    cur.execute("""
        UPDATE discovered_tokens SET
            description = COALESCE(description, %s),
            image_url   = COALESCE(image_url,   %s),
            twitter     = COALESCE(twitter,     %s),
            telegram    = COALESCE(telegram,    %s),
            website     = COALESCE(website,     %s)
        WHERE mint = %s
    """, (
        social["description"], social["image_url"],
        social["twitter"], social["telegram"], social["website"],
        mint
    ))


def run():
    conn = get_db()
    cur  = conn.cursor()

    mints = get_batch(cur)
    if not mints:
        log.info("✅ Sin tokens pendientes de backfill")
        conn.close()
        return

    log.info(f"🔄 Backfill socials: {len(mints)} tokens")

    found = 0
    for i, mint in enumerate(mints, 1):
        try:
            social = process_token(mint)
            if social:
                save(cur, mint, social)
                found += 1
                tw = social["twitter"] or ""
                log.info(
                    f"  [{i}/{len(mints)}] {mint[:12]}... "
                    f"| tw={'✓' if social['twitter'] else '·'} "
                    f"| tg={'✓' if social['telegram'] else '·'} "
                    f"| web={'✓' if social['website'] else '·'} "
                    f"| desc={'✓' if social['description'] else '·'}"
                )
            # Commit every 20 tokens to not lose progress
            if i % 20 == 0:
                conn.commit()
            time.sleep(0.1)
        except Exception as e:
            log.warning(f"  [{i}] {mint[:12]}... error: {e}")

    conn.commit()
    cur.close()
    conn.close()

    log.info(f"✅ Batch completado: {found}/{len(mints)} con socials encontrados")


if __name__ == "__main__":
    run()
