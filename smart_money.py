import requests
import psycopg2
import psycopg2.pool
import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/smart_money.log'),
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

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallet_activity (
                id SERIAL PRIMARY KEY,
                wallet TEXT NOT NULL,
                mint TEXT NOT NULL,
                action TEXT NOT NULL,
                slot BIGINT,
                signature TEXT,
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(wallet, mint, action)
            );
            CREATE TABLE IF NOT EXISTS smart_wallets (
                id SERIAL PRIMARY KEY,
                wallet TEXT UNIQUE NOT NULL,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                total_trades INTEGER DEFAULT 0,
                win_rate NUMERIC(5,2) DEFAULT 0,
                avg_return NUMERIC(10,2) DEFAULT 0,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_smart BOOLEAN DEFAULT FALSE
            );
            CREATE INDEX IF NOT EXISTS idx_wallet_activity_wallet 
                ON wallet_activity(wallet);
            CREATE INDEX IF NOT EXISTS idx_wallet_activity_mint 
                ON wallet_activity(mint);
            CREATE INDEX IF NOT EXISTS idx_smart_wallets_winrate 
                ON smart_wallets(win_rate DESC);
        """)
        conn.commit()
        cur.close()
        log.info("✅ Tablas de smart money creadas")
    finally:
        pool.putconn(conn)

def get_transaction_details(signature):
    """Obtiene los detalles de una transacción para extraer wallets"""
    payload = {
        "jsonrpc": "2.0",
        "id": signature,
        "method": "getTransaction",
        "params": [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
        ]
    }
    try:
        response = requests.post(RPC_URL, json=payload, timeout=10)
        data = response.json()
        return data.get("result")
    except Exception as e:
        log.warning(f"Error obteniendo tx {signature[:20]}: {e}")
        return None

def extract_buyers(signatures, mint):
    """Extrae las wallets que compraron en las primeras transacciones"""
    buyers = []
    for sig_info in signatures[:20]:  # primeras 20 txs
        sig = sig_info['signature']
        slot = sig_info['slot']
        
        tx = get_transaction_details(sig)
        if not tx:
            continue
            
        try:
            # Extraer accounts involucradas en la transacción
            accounts = tx.get('transaction', {}).get('message', {}).get('accountKeys', [])
            
            for account in accounts:
                if isinstance(account, dict):
                    pubkey = account.get('pubkey', '')
                    signer = account.get('signer', False)
                    writable = account.get('writable', False)
                    
                    # El comprador es quien firma y tiene cuenta writable
                    if signer and writable and len(pubkey) > 30:
                        buyers.append({
                            'wallet': pubkey,
                            'signature': sig,
                            'slot': slot
                        })
                        break
        except Exception as e:
            continue
        
        time.sleep(0.1)  # Rate limiting
    
    return buyers

def get_signatures(mint, limit=20):
    """Obtiene las firmas de transacciones de un token"""
    payload = {
        "jsonrpc": "2.0",
        "id": mint,
        "method": "getSignaturesForAddress",
        "params": [mint, {"limit": limit}]
    }
    try:
        response = requests.post(RPC_URL, json=payload, timeout=10)
        data = response.json()
        return data.get("result", [])
    except Exception as e:
        log.warning(f"Error obteniendo signatures para {mint[:20]}: {e}")
        return []

def save_wallet_activity(wallet, mint, action, slot, signature):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO wallet_activity (wallet, mint, action, slot, signature)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (wallet, mint, action) DO NOTHING
        """, (wallet, mint, action, slot, signature))
        conn.commit()
        cur.close()
    except Exception as e:
        log.error(f"Error guardando wallet activity: {e}")
    finally:
        pool.putconn(conn)

def update_smart_wallet(wallet, won):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO smart_wallets (wallet, wins, losses, total_trades, last_seen)
            VALUES (%s, %s, %s, 1, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                wins = smart_wallets.wins + %s,
                losses = smart_wallets.losses + %s,
                total_trades = smart_wallets.total_trades + 1,
                last_seen = NOW()
        """, (
            wallet,
            1 if won else 0,
            0 if won else 1,
            1 if won else 0,
            0 if won else 1
        ))
        # Actualizar win rate
        cur.execute("""
            UPDATE smart_wallets 
            SET win_rate = (wins::float / total_trades * 100),
                is_smart = (wins::float / total_trades > 0.6 AND total_trades >= 3)
            WHERE wallet = %s
        """, (wallet,))
        conn.commit()
        cur.close()
    except Exception as e:
        log.error(f"Error actualizando smart wallet: {e}")
    finally:
        pool.putconn(conn)

def get_winning_tokens():
    """Tokens que tuvieron buen rendimiento"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT dt.mint, dt.name, dt.volume_24h, dt.market_cap
            FROM discovered_tokens dt
            WHERE dt.volume_24h > 5000
            AND dt.buys_5m > 5
            AND dt.created_at > NOW() - INTERVAL '5 days'
            ORDER BY dt.volume_24h DESC
            LIMIT 30
        """)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def analyze_winning_tokens():
    """Analiza los tokens ganadores para encontrar smart wallets"""
    winners = get_winning_tokens()
    log.info(f"🏆 Analizando {len(winners)} tokens ganadores...")

    wallet_wins = {}

    for mint, name, volume, mcap in winners:
        log.info(f"📡 Analizando {name or mint[:12]} | Vol: ${volume:,.0f}")
        
        signatures = get_signatures(mint, limit=20)
        if not signatures:
            continue
        
        buyers = extract_buyers(signatures, mint)
        
        for buyer in buyers:
            wallet = buyer['wallet']
            save_wallet_activity(wallet, mint, 'BUY', buyer['slot'], buyer['signature'])
            
            if wallet not in wallet_wins:
                wallet_wins[wallet] = 0
            wallet_wins[wallet] += 1
            
            update_smart_wallet(wallet, won=True)
        
        time.sleep(0.5)

    # Mostrar top smart wallets
    print_smart_wallets()

def print_smart_wallets():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT wallet, wins, total_trades, win_rate, is_smart, last_seen
            FROM smart_wallets
            WHERE total_trades >= 2
            ORDER BY wins DESC, win_rate DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        cur.close()

        print("\n" + "="*70)
        print(f"🧠 SMART MONEY WALLETS — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("="*70)
        for wallet, wins, total, win_rate, is_smart, last_seen in rows:
            badge = "🔥 SMART" if is_smart else "👀 WATCH"
            print(f"  {badge} | {wallet[:20]}... | "
                  f"Wins: {wins}/{total} | "
                  f"WinRate: {win_rate:.0f}%")
        print("="*70 + "\n")
    finally:
        pool.putconn(conn)

if __name__ == "__main__":
    setup_db()
    log.info("🧠 Smart Money Detector iniciando...")
    analyze_winning_tokens()
    log.info("✅ Análisis completado")
