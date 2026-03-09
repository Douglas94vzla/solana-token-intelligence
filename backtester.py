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
        logging.FileHandler('/var/log/solana_bot/backtester.log'),
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

# ── PARÁMETROS DE ESTRATEGIA ──────────────────────────
CAPITAL = 100.0          # Capital inicial en USD
BET_SIZE = 10.0          # Cuánto arriesgar por trade
TAKE_PROFIT = 999.0        # Salir cuando el precio 3x
STOP_LOSS = 0.5          # Salir cuando pierde 50%
MAX_HOLD_MINUTES = 120    # Salir después de 60 min pase lo que pase

# ── FILTROS DE ENTRADA ────────────────────────────────
MIN_BUYS_5M = 5          # Mínimo de compras en 5 min
MAX_SELLS_5M = 2         # Máximo de ventas en 5 min (0 ideal)
MAX_MCAP = 10000         # MCap máximo en USD
MIN_MCAP = 1000          # MCap mínimo en USD

def get_candidates():
    """Tokens que cumplen los criterios de entrada"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                mint, name, symbol,
                price_usd, market_cap,
                buys_5m, sells_5m,
                volume_24h, created_at
            FROM discovered_tokens
            WHERE buys_5m >= %s
            AND (sells_5m IS NULL OR sells_5m <= %s)
            AND market_cap >= %s
            AND market_cap <= %s
            AND price_usd IS NOT NULL
            AND created_at > NOW() - INTERVAL '5 days'
            AND mint IN (
                SELECT mint FROM price_snapshots 
                GROUP BY mint HAVING COUNT(*) >= 50
            )
            ORDER BY created_at DESC
        """, (MIN_BUYS_5M, MAX_SELLS_5M, MIN_MCAP, MAX_MCAP))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def get_price_evolution(mint, entry_time):
    """Obtiene snapshots de precio después de la entrada"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT price_usd, market_cap, snapshot_at
            FROM price_snapshots
            WHERE mint = %s
            AND snapshot_at > %s
            ORDER BY snapshot_at ASC
        """, (mint, entry_time))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def simulate_trade(token, capital_remaining):
    """Simula un trade completo con take profit y stop loss"""
    mint, name, symbol, entry_price, entry_mcap, buys, sells, volume, created_at = token
    
    if not entry_price or float(entry_price) <= 0:
        return None
    
    entry_price = float(entry_price)
    entry_mcap = float(entry_mcap) if entry_mcap else 0
    bet = min(BET_SIZE, capital_remaining)
    
    # Simular slippage del 3% en la entrada
    actual_entry_price = entry_price * 1.03
    tokens_bought = bet / actual_entry_price
    
    # Obtener evolución del precio
    snapshots = get_price_evolution(mint, created_at)
    
    if not snapshots:
        # Sin snapshots = token murió, asumimos stop loss
        exit_price = actual_entry_price * STOP_LOSS
        exit_reason = 'NO_DATA_STOP'
        hold_minutes = MAX_HOLD_MINUTES
    else:
        exit_price = actual_entry_price
        exit_reason = 'TIMEOUT'
        hold_minutes = MAX_HOLD_MINUTES
        
        for snap_price, snap_mcap, snap_time in snapshots:
            if not snap_price:
                continue
            curr_price = float(snap_price)
            change = curr_price / actual_entry_price
            minutes = (snap_time - created_at).total_seconds() / 60
            
            if minutes > MAX_HOLD_MINUTES:
                exit_price = curr_price
                exit_reason = 'TIMEOUT'
                hold_minutes = minutes
                break
            
            if change >= TAKE_PROFIT:
                exit_price = curr_price
                exit_reason = 'TAKE_PROFIT'
                hold_minutes = minutes
                break
            
            if change <= STOP_LOSS:
                exit_price = curr_price
                exit_reason = 'STOP_LOSS'
                hold_minutes = minutes
                break
            
            exit_price = curr_price
            hold_minutes = minutes

    # Calcular P&L
    exit_value = tokens_bought * exit_price
    pnl = exit_value - bet
    pnl_pct = (exit_value - bet) / bet * 100
    
    return {
        'mint': mint,
        'name': name or mint[:8],
        'symbol': symbol or '???',
        'entry_price': actual_entry_price,
        'exit_price': exit_price,
        'entry_mcap': entry_mcap,
        'buys_5m': buys,
        'sells_5m': sells,
        'bet': bet,
        'exit_value': exit_value,
        'pnl': pnl,
        'pnl_pct': pnl_pct,
        'exit_reason': exit_reason,
        'hold_minutes': hold_minutes,
        'created_at': created_at
    }

def run_backtest():
    candidates = get_candidates()
    log.info(f"📊 Candidatos encontrados: {len(candidates)}")
    
    if not candidates:
        log.warning("Sin candidatos — necesitamos más snapshots de precio")
        return

    capital = CAPITAL
    trades = []
    wins = 0
    losses = 0
    total_pnl = 0

    for token in candidates:
        if capital < BET_SIZE:
            log.info("Capital insuficiente para seguir")
            break
        
        result = simulate_trade(token, capital)
        if not result:
            continue
        
        capital += result['pnl']
        total_pnl += result['pnl']
        trades.append(result)
        
        if result['pnl'] > 0:
            wins += 1
        else:
            losses += 1

    # ── REPORTE FINAL ─────────────────────────────────
    print("\n" + "="*70)
    print(f"📊 BACKTEST REPORT — Estrategia: buys≥{MIN_BUYS_5M}, sells≤{MAX_SELLS_5M}, TP:{TAKE_PROFIT}x, SL:{STOP_LOSS}x")
    print("="*70)
    print(f"  Capital inicial:    ${CAPITAL:.2f}")
    print(f"  Capital final:      ${capital:.2f}")
    print(f"  P&L Total:          ${total_pnl:+.2f}")
    print(f"  ROI:                {(capital-CAPITAL)/CAPITAL*100:+.1f}%")
    print(f"  Total trades:       {len(trades)}")
    print(f"  Wins:               {wins} ({wins/len(trades)*100:.1f}%)" if trades else "  Sin trades")
    print(f"  Losses:             {losses} ({losses/len(trades)*100:.1f}%)" if trades else "")
    print()

    # Top winners
    winners = sorted([t for t in trades if t['pnl'] > 0], key=lambda x: x['pnl_pct'], reverse=True)
    print(f"🏆 TOP 5 GANADORES:")
    for t in winners[:5]:
        print(f"   {t['name']:<20} +{t['pnl_pct']:.0f}% | ${t['pnl']:+.2f} | {t['exit_reason']} | {t['hold_minutes']:.0f}min")

    print()
    losers = sorted([t for t in trades if t['pnl'] < 0], key=lambda x: x['pnl_pct'])
    print(f"💀 TOP 5 PERDEDORES:")
    for t in losers[:5]:
        print(f"   {t['name']:<20} {t['pnl_pct']:.0f}% | ${t['pnl']:+.2f} | {t['exit_reason']} | {t['hold_minutes']:.0f}min")

    print()
    reasons = {}
    for t in trades:
        reasons[t['exit_reason']] = reasons.get(t['exit_reason'], 0) + 1
    print(f"📋 SALIDAS POR RAZÓN:")
    for reason, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
        print(f"   {reason:<20} {count} trades")

    print("="*70 + "\n")
    return trades

if __name__ == "__main__":
    log.info("🔬 Backtester iniciando...")
    run_backtest()
    log.info("✅ Backtest completado")

# OVERRIDE: solo tokens con snapshots suficientes
import sys
sys.exit(0)
