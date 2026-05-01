#!/usr/bin/env python3
"""
Auditoría completa del sistema Solana Bot - 29 de abril 2026
Extrae datos reales de PostgreSQL sin adornos
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Cargar variables de entorno
load_dotenv('/root/solana_bot/.env')

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'solana_bot')
DB_USER = os.getenv('DB_USER', 'solana_user')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT', '5432')

def connect_db():
    """Conectar a PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"ERROR: No se pudo conectar a la base de datos: {e}")
        sys.exit(1)

def audit_financial_performance(conn):
    """SECCIÓN 1: RENDIMIENTO FINANCIERO"""
    print("\n" + "="*80)
    print("SECCIÓN 1 — RENDIMIENTO FINANCIERO")
    print("="*80)
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Capital inicial vs actual
        cur.execute("""
            SELECT 
                SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as total_wins,
                SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END) as total_losses,
                COUNT(*) as total_trades,
                SUM(pnl) as net_pnl,
                MIN(created_at) as first_trade_date,
                MAX(closed_at) as last_trade_date
            FROM paper_trades
            WHERE status = 'CLOSED'
        """)
        perf = cur.fetchone()
        
        print(f"\n✓ Total trades cerrados: {perf['total_trades']}")
        print(f"✓ Total ganancias (sum): ${perf['total_wins']:.2f}" if perf['total_wins'] else "✓ Total ganancias: $0.00")
        print(f"✓ Total pérdidas (sum): ${perf['total_losses']:.2f}" if perf['total_losses'] else "✓ Total pérdidas: $0.00")
        print(f"✓ PnL neto: ${perf['net_pnl']:.2f}" if perf['net_pnl'] else "✓ PnL neto: $0.00")
        print(f"✓ Período: {perf['first_trade_date']} → {perf['last_trade_date']}")
        
        # Win rate últimos 29 días vs primeros 29 días
        cur.execute("""
            SELECT 
                DATE(created_at) as trade_date,
                COUNT(*) as trades_day,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins_day
            FROM paper_trades
            WHERE status = 'CLOSED'
            GROUP BY DATE(created_at)
            ORDER BY DATE(created_at) ASC
        """)
        daily_trades = cur.fetchall()
        
        if len(daily_trades) >= 29:
            first_29 = daily_trades[:29]
            last_29 = daily_trades[-29:]
            
            first_29_wr = sum(d['wins_day'] for d in first_29) / sum(d['trades_day'] for d in first_29) * 100 if sum(d['trades_day'] for d in first_29) > 0 else 0
            last_29_wr = sum(d['wins_day'] for d in last_29) / sum(d['trades_day'] for d in last_29) * 100 if sum(d['trades_day'] for d in last_29) > 0 else 0
            
            print(f"\n✓ Win rate primeros 29 días: {first_29_wr:.1f}%")
            print(f"✓ Win rate últimos 29 días: {last_29_wr:.1f}%")
            print(f"✓ Mejora: {last_29_wr - first_29_wr:+.1f}%")
        
        # Distribución de exits
        cur.execute("""
            SELECT 
                exit_reason,
                COUNT(*) as count,
                SUM(pnl) as total_pnl,
                AVG(pnl) as avg_pnl
            FROM paper_trades
            WHERE status = 'CLOSED'
            GROUP BY exit_reason
            ORDER BY count DESC
        """)
        exits = cur.fetchall()
        
        print(f"\n✓ Distribución de exits:")
        for exit_type in exits:
            print(f"  - {exit_type['exit_reason']}: {exit_type['count']} trades, PnL promedio ${exit_type['avg_pnl']:.2f}")
        
        # Top 10 mejores y peores trades
        cur.execute("""
            SELECT mint, entry_price, exit_price, pnl, exit_reason, closed_at
            FROM paper_trades
            WHERE status = 'CLOSED'
            ORDER BY pnl DESC
            LIMIT 10
        """)
        best_trades = cur.fetchall()
        
        cur.execute("""
            SELECT mint, entry_price, exit_price, pnl, exit_reason, closed_at
            FROM paper_trades
            WHERE status = 'CLOSED'
            ORDER BY pnl ASC
            LIMIT 10
        """)
        worst_trades = cur.fetchall()
        
        print(f"\n✓ Top 10 MEJORES trades:")
        for i, trade in enumerate(best_trades, 1):
            print(f"  {i}. {trade['mint'][:8]}... → ${trade['pnl']:.2f} ({trade['exit_reason']})")
        
        print(f"\n✓ Top 10 PEORES trades:")
        for i, trade in enumerate(worst_trades, 1):
            print(f"  {i}. {trade['mint'][:8]}... → ${trade['pnl']:.2f} ({trade['exit_reason']})")
        
    except Exception as e:
        print(f"ERROR en análisis financiero: {e}")

def audit_signal_quality(conn):
    """SECCIÓN 2: CALIDAD DE LAS SEÑALES"""
    print("\n" + "="*80)
    print("SECCIÓN 2 — CALIDAD DE LAS SEÑALES")
    print("="*80)
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Señales ENTER por día
        cur.execute("""
            SELECT 
                DATE(created_at) as signal_date,
                COUNT(*) as signals_day,
                strategy
            FROM discovered_tokens
            WHERE signal = 'ENTER'
            GROUP BY DATE(created_at), strategy
            ORDER BY DATE(created_at)
        """)
        enter_signals = cur.fetchall()
        
        print(f"\n✓ Señales ENTER totales: {len(enter_signals)}")
        
        if enter_signals:
            signals_per_day = sum(d['signals_day'] for d in enter_signals) / len(set(d['signal_date'] for d in enter_signals))
            print(f"✓ Promedio por día: {signals_per_day:.1f} señales")
        
        # Win rate por estrategia
        cur.execute("""
            SELECT 
                dt.strategy,
                COUNT(pt.id) as trades,
                SUM(CASE WHEN pt.pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pt.pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(pt.id) * 100 as win_rate,
                AVG(pt.pnl) as avg_pnl
            FROM discovered_tokens dt
            LEFT JOIN paper_trades pt ON dt.mint = pt.mint
            WHERE pt.status = 'CLOSED'
            GROUP BY dt.strategy
        """)
        strategy_perf = cur.fetchall()
        
        print(f"\n✓ Performance por estrategia:")
        for strat in strategy_perf:
            print(f"  - {strat['strategy']}: {strat['trades']} trades, WR {strat['win_rate']:.1f}%, PnL promedio ${strat['avg_pnl']:.2f}")
        
    except Exception as e:
        print(f"ERROR en análisis de señales: {e}")

def audit_system_health(conn):
    """SECCIÓN 3: SALUD DEL SISTEMA"""
    print("\n" + "="*80)
    print("SECCIÓN 3 — SALUD DEL SISTEMA")
    print("="*80)
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Estado del ML
        cur.execute("""
            SELECT COUNT(*) as total_tokens, COUNT(ml_probability) as tokens_with_ml
            FROM discovered_tokens
            WHERE ml_probability IS NOT NULL
        """)
        ml_stats = cur.fetchone()
        
        print(f"\n✓ Cobertura ML: {ml_stats['tokens_with_ml']}/{ml_stats['total_tokens']} tokens con predicción")
        
        # Datos faltantes
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(deployer_wallet) as has_deployer,
                COUNT(funding_wallet) as has_funding,
                COUNT(liquidity_usd) as has_liquidity
            FROM discovered_tokens
        """)
        data_coverage = cur.fetchone()
        
        if data_coverage['total'] > 0:
            print(f"\n✓ Cobertura de datos:")
            print(f"  - deployer_wallet: {data_coverage['has_deployer']/data_coverage['total']*100:.1f}%")
            print(f"  - funding_wallet: {data_coverage['has_funding']/data_coverage['total']*100:.1f}%")
            print(f"  - liquidity_usd: {data_coverage['has_liquidity']/data_coverage['total']*100:.1f}%")
        
    except Exception as e:
        print(f"ERROR en análisis de salud: {e}")

def audit_market_analysis(conn):
    """SECCIÓN 4: ANÁLISIS DEL MERCADO"""
    print("\n" + "="*80)
    print("SECCIÓN 4 — ANÁLISIS DEL MERCADO")
    print("="*80)
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Distribución por narrativa
        cur.execute("""
            SELECT 
                narrative,
                COUNT(*) as count,
                SUM(CASE WHEN signal = 'ENTER' THEN 1 ELSE 0 END) as enters
            FROM discovered_tokens
            WHERE narrative IS NOT NULL
            GROUP BY narrative
            ORDER BY count DESC
            LIMIT 10
        """)
        narratives = cur.fetchall()
        
        print(f"\n✓ Top narrativas (últimos 29 días):")
        for narr in narratives:
            print(f"  - {narr['narrative']}: {narr['count']} tokens, {narr['enters']} ENTER signals")
        
        # Tasa base
        cur.execute("""
            SELECT 
                COUNT(*) as total_tokens,
                COUNT(CASE WHEN max_price >= entry_price * 1.3 THEN 1 END) as tokens_30pct
            FROM discovered_tokens
            LEFT JOIN (
                SELECT mint, MAX(price) as max_price FROM price_snapshots GROUP BY mint
            ) ps ON discovered_tokens.mint = ps.mint
        """)
        base_rate = cur.fetchone()
        
        if base_rate['total_tokens'] > 0:
            print(f"\n✓ Tasa base: {base_rate['tokens_30pct']}/{base_rate['total_tokens']} tokens llegaron a +30%")
            print(f"  Porcentaje: {base_rate['tokens_30pct']/base_rate['total_tokens']*100:.2f}%")
        
    except Exception as e:
        print(f"ERROR en análisis de mercado: {e}")

def audit_verdict(conn):
    """SECCIÓN 5: VEREDICTO"""
    print("\n" + "="*80)
    print("SECCIÓN 5 — VEREDICTO")
    print("="*80)
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cur.execute("""
            SELECT 
                COUNT(*) as total_trades,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as win_rate,
                AVG(pnl) as avg_pnl,
                STDDEV(pnl) as stddev_pnl,
                SUM(pnl) as total_pnl
            FROM paper_trades
            WHERE status = 'CLOSED'
        """)
        summary = cur.fetchone()
        
        print(f"\n✓ RESUMEN EJECUTIVO:")
        print(f"  - Trades totales: {summary['total_trades']}")
        print(f"  - Win rate: {summary['win_rate']:.1f}%")
        print(f"  - PnL promedio: ${summary['avg_pnl']:.2f}")
        print(f"  - PnL total: ${summary['total_pnl']:.2f}")
        print(f"  - Volatilidad (StdDev): ${summary['stddev_pnl']:.2f}")
        
        # Ready for live?
        is_ready = summary['win_rate'] > 55 and summary['total_trades'] >= 50
        print(f"\n✓ ¿Listo para LIVE TRADING? {'SÍ' if is_ready else 'NO'}")
        print(f"  Justificación:")
        print(f"    - Mínimo 50 trades requeridos: {summary['total_trades']}/50 ✓" if summary['total_trades'] >= 50 else f"    - Mínimo 50 trades requeridos: {summary['total_trades']}/50 ✗")
        print(f"    - Win rate >55% requerido: {summary['win_rate']:.1f}% ✓" if summary['win_rate'] > 55 else f"    - Win rate >55% requerido: {summary['win_rate']:.1f}% ✗")
        
    except Exception as e:
        print(f"ERROR en veredicto: {e}")

def main():
    print("\n" + "🔍 AUDITORÍA DEL SISTEMA SOLANA BOT — 29 DE ABRIL 2026".center(80, "="))
    
    conn = connect_db()
    
    audit_financial_performance(conn)
    audit_signal_quality(conn)
    audit_system_health(conn)
    audit_market_analysis(conn)
    audit_verdict(conn)
    
    conn.close()
    
    print("\n" + "="*80)
    print("FIN DE LA AUDITORÍA".center(80))
    print("="*80 + "\n")

if __name__ == '__main__':
    main()
