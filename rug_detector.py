import requests
import psycopg2
import psycopg2.pool
import os
import logging
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/rug_detector.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

RPC_URL = os.getenv("RPC_URL")
# RPC público como fallback para métodos que Helius rate-limita (getSignaturesForAddress)
PUBLIC_RPC_URL = "https://api.mainnet-beta.solana.com"

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
                ADD COLUMN IF NOT EXISTS rug_score INTEGER DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS rug_flags TEXT DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS top10_concentration NUMERIC(5,2) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS holder_count INTEGER DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS dev_sold BOOLEAN DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS rug_checked_at TIMESTAMP DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS funding_wallet TEXT DEFAULT NULL
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"ALTER TABLE skipped (lock timeout): {e}")
        cur.close()
        log.info("✅ Columnas de rug detector añadidas")
    finally:
        pool.putconn(conn)

def get_deployer_history(mint):
    """
    Consulta el historial del deployer de este token.
    Retorna (is_serial_rugger, total_tokens, rugged_count, rug_rate) o None.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT ds.is_serial_rugger, ds.total_tokens, ds.rugged_count, ds.rug_rate
            FROM discovered_tokens dt
            JOIN deployer_stats ds ON ds.wallet = dt.deployer_wallet
            WHERE dt.mint = %s
        """, (mint,))
        return cur.fetchone()
    except Exception:
        return None
    finally:
        pool.putconn(conn)

def get_token_largest_accounts(mint):
    """Obtiene los top holders del token"""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [mint]
    }
    try:
        resp = requests.post(RPC_URL, json=payload, timeout=10).json()
        if "error" in resp:
            return None  # RPC error (rate limit, etc.) — treat as unknown
        return resp.get("result", {}).get("value", [])
    except Exception:
        return None

def get_token_supply(mint):
    """Obtiene el supply total del token"""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getTokenSupply",
        "params": [mint]
    }
    try:
        resp = requests.post(RPC_URL, json=payload, timeout=10).json()
        return float(resp.get("result", {}).get("value", {}).get("uiAmount", 0))
    except Exception:
        return 0

def get_token_mint_info(mint):
    """Verifica si el mint authority fue revocado"""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getAccountInfo",
        "params": [mint, {"encoding": "jsonParsed"}]
    }
    try:
        resp = requests.post(RPC_URL, json=payload, timeout=10).json()
        info = resp.get("result", {}).get("value", {}).get("data", {}).get("parsed", {}).get("info", {})
        return {
            "mint_authority": info.get("mintAuthority"),
            "freeze_authority": info.get("freezeAuthority"),
            "decimals": info.get("decimals", 0),
        }
    except Exception:
        return {}

def get_recent_large_sells(mint):
    """Detecta ventas grandes recientes usando DexScreener. Retorna también liquidez y volumen."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=8).json()
        pairs = resp.get("pairs")
        if not pairs:
            return False, 0, 0, 0, 0
        pair = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
        buys_5m   = pair.get("txns", {}).get("m5", {}).get("buys", 0)
        sells_5m  = pair.get("txns", {}).get("m5", {}).get("sells", 0)
        buys_1h   = pair.get("txns", {}).get("h1", {}).get("buys", 0)
        sells_1h  = pair.get("txns", {}).get("h1", {}).get("sells", 0)
        volume_24h = pair.get("volume", {}).get("h24", 0) or 0
        liquidity  = (pair.get("liquidity") or {}).get("usd", 0) or 0
        sell_pressure = sells_1h > buys_1h * 2 and sells_1h > 10
        return sell_pressure, buys_5m, sells_5m, float(volume_24h), float(liquidity)
    except Exception:
        return False, 0, 0, 0, 0

def get_deployer_wallet_for_mint(mint):
    """Obtiene la wallet del deployer desde discovered_tokens."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT deployer_wallet, funding_wallet FROM discovered_tokens WHERE mint = %s", (mint,))
        row = cur.fetchone()
        cur.close()
        return (row[0], row[1]) if row else (None, None)
    except Exception:
        return (None, None)
    finally:
        pool.putconn(conn)


def get_funding_wallet(deployer_wallet):
    """
    Obtiene la wallet que financió al deployer (primera tx de SOL recibida).
    Usa el RPC público de Solana para evitar rate limits de Helius en
    getSignaturesForAddress + getTransaction.
    """
    if not deployer_wallet:
        return None
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getSignaturesForAddress",
            "params": [deployer_wallet, {"limit": 20, "commitment": "confirmed"}]
        }
        resp = requests.post(PUBLIC_RPC_URL, json=payload, timeout=10).json()
        if "error" in resp:
            return None
        signatures = resp.get("result", [])
        if not signatures:
            return None

        # Examinar las más antiguas (al final de la lista)
        for sig_info in reversed(signatures[-10:]):
            sig = sig_info.get("signature")
            if not sig:
                continue
            try:
                tx_payload = {
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getTransaction",
                    "params": [sig, {
                        "encoding": "jsonParsed",
                        "commitment": "confirmed",
                        "maxSupportedTransactionVersion": 0
                    }]
                }
                tx_resp = requests.post(PUBLIC_RPC_URL, json=tx_payload, timeout=10).json()
                tx = tx_resp.get("result")
                if not tx:
                    continue

                meta          = tx.get("meta", {})
                pre_balances  = meta.get("preBalances", [])
                post_balances = meta.get("postBalances", [])
                account_keys  = (tx.get("transaction", {})
                                   .get("message", {})
                                   .get("accountKeys", []))

                for i, key_info in enumerate(account_keys):
                    key = key_info.get("pubkey") if isinstance(key_info, dict) else key_info
                    if (key == deployer_wallet
                            and i < len(pre_balances)
                            and i < len(post_balances)):
                        # El deployer recibió SOL (al menos 0.001 SOL)
                        if post_balances[i] > pre_balances[i] + 1_000_000:
                            sender_info = account_keys[0] if account_keys else None
                            if sender_info:
                                sender = (sender_info.get("pubkey")
                                          if isinstance(sender_info, dict)
                                          else sender_info)
                                if sender and sender != deployer_wallet:
                                    return sender
            except Exception:
                continue
        return None
    except Exception as e:
        log.debug(f"Error obteniendo funding wallet para {deployer_wallet[:8]}: {e}")
        return None


def check_funding_wallet_rugs(funding_wallet):
    """
    Cuenta cuántos tokens con rug_score >= 60 están asociados a esta funding wallet.
    """
    if not funding_wallet:
        return 0
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(DISTINCT mint)
            FROM discovered_tokens
            WHERE funding_wallet = %s
              AND rug_score >= 60
        """, (funding_wallet,))
        row = cur.fetchone()
        cur.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0
    finally:
        pool.putconn(conn)


def analyze_rug_risk(mint):
    """
    Analiza el riesgo de rug de un token.
    
    Score de 0-100:
    - 0-30:  SAFE — bajo riesgo
    - 31-60: CAUTION — riesgo medio
    - 61-80: DANGER — alto riesgo  
    - 81-100: RUG — evitar
    """
    flags = []
    risk_score = 0

    # ── 1. CONCENTRACIÓN DE HOLDERS ──────────────────
    holders = get_token_largest_accounts(mint)
    supply = get_token_supply(mint)

    top10_pct = 0
    # holders is None when RPC returned an error (rate limit, etc.) — skip holder checks
    holder_count = len(holders) if holders is not None else None

    if holders and supply > 0:
        top10_amount = sum(float(h.get("uiAmount", 0)) for h in holders[:10])
        top10_pct = (top10_amount / supply * 100) if supply > 0 else 0

        if top10_pct > 95 and holder_count < 10:
            risk_score += 40
            flags.append(f"TOP10_HOLDS_{top10_pct:.0f}%")
        elif top10_pct > 90 and holder_count < 15:
            risk_score += 25
            flags.append(f"TOP10_HOLDS_{top10_pct:.0f}%")
        elif top10_pct > 80 and holder_count < 20:
            risk_score += 10
            flags.append(f"TOP10_HOLDS_{top10_pct:.0f}%")

    # ── 2. MINT AUTHORITY ────────────────────────────
    mint_info = get_token_mint_info(mint)
    mint_authority = mint_info.get("mint_authority")
    freeze_authority = mint_info.get("freeze_authority")
    
    if mint_authority:
        risk_score += 20
        flags.append("MINT_AUTHORITY_ACTIVE")  # Pueden crear más tokens
    
    if freeze_authority:
        risk_score += 15
        flags.append("FREEZE_AUTHORITY_ACTIVE")  # Pueden congelar wallets

    # ── 3. PRESIÓN VENDEDORA + LIQUIDEZ ──────────────
    sell_pressure, buys_5m, sells_5m, volume_24h, liquidity = get_recent_large_sells(mint)

    if sell_pressure:
        risk_score += 25
        flags.append("HEAVY_SELL_PRESSURE")

    if sells_5m > buys_5m * 3 and sells_5m > 5:
        risk_score += 15
        flags.append(f"SELLS_DOMINATING_{sells_5m}vs{buys_5m}")

    # ── 4b. VOLUMEN/LIQUIDEZ DESPROPORCIONADOS ────────
    # Vol >> liquidez indica manipulación o liquidez casi vacía
    if liquidity > 0 and volume_24h > liquidity * 50:
        risk_score += 20
        flags.append(f"VOL_LIQ_MISMATCH_{int(volume_24h/max(liquidity,1))}x")
    elif liquidity < 500 and volume_24h > 5000:
        risk_score += 15
        flags.append("NEAR_ZERO_LIQUIDITY")

    # ── 5. POCOS HOLDERS ─────────────────────────────
    if holder_count is not None:
        if holder_count < 5:
            risk_score += 20
            flags.append(f"ONLY_{holder_count}_HOLDERS")
        elif holder_count < 10:
            risk_score += 10
            flags.append(f"FEW_HOLDERS_{holder_count}")

    # ── 6. HISTORIAL DEL DEPLOYER ────────────────────
    deployer_info = get_deployer_history(mint)
    if deployer_info:
        is_serial, total, rugged, rate = deployer_info
        if is_serial:
            risk_score += 35
            flags.append(f"KNOWN_RUGGER_{rugged}of{total}tokens")
        elif rate >= 0.30 and total >= 2:
            risk_score += 15
            flags.append(f"SUSPICIOUS_DEPLOYER_{rugged}of{total}tokens")

    # ── 7. FUNDING WALLET (SERIAL RUGGER POR WALLET MADRE) ────
    # El deployer de cada rug es una wallet nueva, pero la wallet que la
    # financió (envió SOL inicial) suele ser siempre la misma.
    funding_wallet = None
    deployer_wallet, saved_funding = get_deployer_wallet_for_mint(mint)
    if deployer_wallet:
        # Reutilizar funding_wallet ya guardada para evitar llamadas RPC repetidas
        funding_wallet = saved_funding or get_funding_wallet(deployer_wallet)
        if funding_wallet:
            rug_count = check_funding_wallet_rugs(funding_wallet)
            if rug_count >= 2:
                penalty = min(40, rug_count * 15)   # 15 pts/rug, cap +40
                risk_score += penalty
                flags.append(f"SERIAL_RUGGER_{rug_count}RUGS")
                log.info(f"🚨 SERIAL RUGGER | {mint[:8]} | "
                         f"Funding: {funding_wallet[:8]} | {rug_count} rugs previos")

    # ── CLASIFICACIÓN FINAL ──────────────────────────
    risk_score = min(risk_score, 100)
    
    if risk_score >= 81:
        verdict = "🔴 RUG"
    elif risk_score >= 61:
        verdict = "🟠 DANGER"
    elif risk_score >= 31:
        verdict = "🟡 CAUTION"
    else:
        verdict = "🟢 SAFE"

    return {
        "rug_score":      risk_score,
        "verdict":        verdict,
        "flags":          ", ".join(flags) if flags else "NONE",
        "top10_pct":      top10_pct,
        "holder_count":   holder_count,
        "mint_authority": bool(mint_authority),
        "freeze_authority": bool(freeze_authority),
        "funding_wallet": funding_wallet,
    }

def save_rug_analysis(mint, analysis):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE discovered_tokens SET
                rug_score           = %s,
                rug_flags           = %s,
                top10_concentration = %s,
                holder_count        = %s,
                funding_wallet      = COALESCE(%s, funding_wallet),
                rug_checked_at      = NOW()
            WHERE mint = %s
        """, (
            analysis["rug_score"],
            analysis["flags"],
            analysis["top10_pct"],
            analysis["holder_count"],
            analysis.get("funding_wallet"),
            mint
        ))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_unanalyzed_tokens():
    """
    Tokens pendientes de análisis rug:
    1. Tokens nuevos sin chequeo (rug_checked_at IS NULL)
    2. Tokens con señal ENTER cuyo chequeo tiene más de 30min de antigüedad
       (para detectar cambios post-entrada: dump, liquidez, sell pressure)
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, name, symbol, market_cap, buys_5m, signal
            FROM discovered_tokens
            WHERE (
                -- Nuevos sin chequear
                (rug_checked_at IS NULL
                 AND created_at > NOW() - INTERVAL '24 hours'
                 AND signal IN ('STRONG_BUY', 'BUY'))
                OR
                -- ENTER signals con chequeo desactualizado (>30min)
                (entry_signal = 'ENTER'
                 AND rug_checked_at < NOW() - INTERVAL '30 minutes'
                 AND created_at > NOW() - INTERVAL '3 hours')
            )
            AND price_usd IS NOT NULL
            ORDER BY
                CASE WHEN entry_signal = 'ENTER' THEN 0 ELSE 1 END,
                survival_score DESC NULLS LAST
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def print_summary():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                CASE 
                    WHEN rug_score >= 81 THEN 'RUG'
                    WHEN rug_score >= 61 THEN 'DANGER'
                    WHEN rug_score >= 31 THEN 'CAUTION'
                    ELSE 'SAFE'
                END as verdict,
                COUNT(*) as total,
                ROUND(AVG(rug_score)::numeric, 1) as avg_score,
                ROUND(AVG(top10_concentration)::numeric, 1) as avg_concentration
            FROM discovered_tokens
            WHERE rug_checked_at IS NOT NULL
            AND created_at > NOW() - INTERVAL '24 hours'
            GROUP BY 1
            ORDER BY avg_score DESC
        """)
        rows = cur.fetchall()
        cur.close()

        print("\n" + "="*60)
        print(f"🛡️  RUG DETECTOR SUMMARY — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("="*60)
        for verdict, total, avg_score, avg_conc in rows:
            print(f"  {verdict:<10} | {total:>4} tokens | "
                  f"Score avg: {avg_score} | "
                  f"Top10 avg: {avg_conc}%")
        print("="*60 + "\n")
    finally:
        pool.putconn(conn)

def get_tokens_missing_funding_wallet():
    """
    Tokens que ya fueron rug-checked pero sin funding_wallet porque
    deployer_wallet llegó después (race condition con metadata_fetcher).
    Limitado a tokens recientes para no sobrecargar el RPC.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint, deployer_wallet
            FROM discovered_tokens
            WHERE deployer_wallet IS NOT NULL
              AND funding_wallet IS NULL
              AND rug_checked_at IS NOT NULL
              AND created_at > NOW() - INTERVAL '48 hours'
            ORDER BY rug_checked_at DESC
            LIMIT 50
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)


def save_funding_wallet(mint, funding_wallet):
    """Actualiza solo la columna funding_wallet de un token ya analizado."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE discovered_tokens SET funding_wallet = %s WHERE mint = %s",
            (funding_wallet, mint)
        )
        conn.commit()
        cur.close()
    except Exception as e:
        conn.rollback()
        log.error(f"Error guardando funding_wallet para {mint[:16]}: {e}")
    finally:
        pool.putconn(conn)


def run():
    setup_db()
    tokens = get_unanalyzed_tokens()
    log.info(f"🛡️  Analizando {len(tokens)} tokens...")

    for mint, name, symbol, mcap, buys, signal in tokens:
        try:
            analysis = analyze_rug_risk(mint)
            save_rug_analysis(mint, analysis)
            time.sleep(0.5)  # evitar rate limit en get_funding_wallet (public RPC)

            verdict = analysis["verdict"]
            score   = analysis["rug_score"]
            top10   = analysis["top10_pct"]
            flags   = analysis["flags"]

            log.info(
                f"{verdict} | Score: {score} | "
                f"{name or mint[:8]} ({symbol}) | "
                f"Top10: {top10:.0f}% | "
                f"Flags: {flags}"
            )
        except Exception as e:
            log.error(f"Error analizando {mint[:16]}: {e}")

    # Segundo pase: rellenar funding_wallet para tokens ya analizados cuyo
    # deployer_wallet llegó después (race condition con metadata_fetcher)
    backfill = get_tokens_missing_funding_wallet()
    if backfill:
        log.info(f"🔍 Backfill funding_wallet: {len(backfill)} tokens con deployer pero sin funding")
        for mint, deployer_wallet in backfill:
            try:
                fw = get_funding_wallet(deployer_wallet)
                if fw:
                    save_funding_wallet(mint, fw)
                    log.info(f"💳 Funding wallet encontrada: {mint[:8]} ← {fw[:8]}")
                time.sleep(0.5)
            except Exception as e:
                log.error(f"Error en backfill funding_wallet {mint[:16]}: {e}")

    print_summary()
    log.info("✅ Análisis completado")

if __name__ == "__main__":
    run()
