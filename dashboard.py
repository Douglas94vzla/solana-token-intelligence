import streamlit as st
import psycopg2
import psycopg2.pool
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import time

load_dotenv()

st.set_page_config(
    page_title="SOLANA INTELLIGENCE",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Share+Tech+Mono&display=swap');

* { margin: 0; padding: 0; box-sizing: border-box; }

html, body, [data-testid="stAppViewContainer"] {
    background-color: #020008 !important;
    background-image: 
        radial-gradient(ellipse at 20% 50%, rgba(0, 255, 163, 0.07) 0%, transparent 50%),
        radial-gradient(ellipse at 80% 20%, rgba(0, 100, 255, 0.07) 0%, transparent 50%),
        repeating-linear-gradient(
            0deg,
            transparent,
            transparent 2px,
            rgba(0, 255, 163, 0.015) 2px,
            rgba(0, 255, 163, 0.015) 4px
        );
}

[data-testid="stHeader"] { background: transparent !important; }

h1, h2, h3 { font-family: 'Orbitron', monospace !important; }

.metric-card {
    background: linear-gradient(135deg, rgba(0,255,163,0.05) 0%, rgba(0,0,0,0.8) 100%);
    border: 1px solid rgba(0, 255, 163, 0.3);
    border-radius: 4px;
    padding: 20px;
    text-align: center;
    position: relative;
    overflow: hidden;
}

.metric-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, #00ffa3, transparent);
}

.metric-value {
    font-family: 'Orbitron', monospace;
    font-size: 2.2rem;
    font-weight: 900;
    color: #00ffa3;
    text-shadow: 0 0 20px rgba(0,255,163,0.6);
    line-height: 1;
}

.metric-label {
    font-family: 'Share Tech Mono', monospace;
    font-size: 0.7rem;
    color: rgba(0,255,163,0.5);
    letter-spacing: 3px;
    text-transform: uppercase;
    margin-top: 8px;
}

.trend-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 10px 16px;
    margin: 4px 0;
    background: rgba(0,255,163,0.03);
    border-left: 2px solid rgba(0,255,163,0.4);
    font-family: 'Share Tech Mono', monospace;
    font-size: 0.85rem;
    color: #00ffa3;
    transition: all 0.2s;
}

.token-row {
    padding: 12px 16px;
    margin: 3px 0;
    background: rgba(255,255,255,0.02);
    border: 1px solid rgba(0,255,163,0.1);
    border-radius: 3px;
    font-family: 'Share Tech Mono', monospace;
    font-size: 0.8rem;
    color: rgba(255,255,255,0.8);
}

.header-title {
    font-family: 'Orbitron', monospace;
    font-size: 2.8rem;
    font-weight: 900;
    color: #00ffa3;
    text-shadow: 0 0 40px rgba(0,255,163,0.4), 0 0 80px rgba(0,255,163,0.2);
    letter-spacing: 6px;
    text-align: center;
}

.header-sub {
    font-family: 'Share Tech Mono', monospace;
    font-size: 0.8rem;
    color: rgba(0,255,163,0.4);
    letter-spacing: 4px;
    text-align: center;
    margin-top: 8px;
}

.section-title {
    font-family: 'Orbitron', monospace;
    font-size: 0.75rem;
    color: rgba(0,255,163,0.5);
    letter-spacing: 4px;
    text-transform: uppercase;
    border-bottom: 1px solid rgba(0,255,163,0.15);
    padding-bottom: 8px;
    margin-bottom: 16px;
}

.positive { color: #00ffa3 !important; }
.negative { color: #ff4466 !important; }
.neutral  { color: #ffaa00 !important; }

div[data-testid="stMetric"] {
    background: transparent !important;
}

.stDataFrame { background: transparent !important; }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_pool():
    return psycopg2.pool.ThreadedConnectionPool(
        1, 5,
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST")
    )

def query(sql, params=None):
    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description] if cur.description else []
        conn.commit()
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"DB Error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return pd.DataFrame()
    finally:
        pool.putconn(conn)

# ── HEADER ──────────────────────────────────────────
st.markdown('<div class="header-title">⚡ SOLANA INTELLIGENCE</div>', unsafe_allow_html=True)
st.markdown('<div class="header-sub">REAL-TIME PUMP.FUN TOKEN SURVEILLANCE SYSTEM</div>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# ── STATS ────────────────────────────────────────────
df_stats = query("""
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN created_at > NOW() - INTERVAL '1 hour' THEN 1 END) as last_hour,
        COUNT(CASE WHEN created_at > NOW() - INTERVAL '24 hours' THEN 1 END) as last_24h,
        COUNT(CASE WHEN name IS NOT NULL AND name != '' THEN 1 END) as with_name,
        COUNT(CASE WHEN price_usd IS NOT NULL THEN 1 END) as with_price
    FROM discovered_tokens
""")

if not df_stats.empty:
    row = df_stats.iloc[0]
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{int(row.total):,}</div><div class="metric-label">Total Tokens</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{int(row.last_hour):,}</div><div class="metric-label">Last Hour</div></div>', unsafe_allow_html=True)
    with c3:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{int(row.last_24h):,}</div><div class="metric-label">Last 24H</div></div>', unsafe_allow_html=True)
    with c4:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{int(row.with_name):,}</div><div class="metric-label">With Metadata</div></div>', unsafe_allow_html=True)
    with c5:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{int(row.with_price):,}</div><div class="metric-label">With Price</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── ROW 2: TRENDS + VOLUME CHART ─────────────────────
col_left, col_right = st.columns([1, 2])

with col_left:
    st.markdown('<div class="section-title">🔥 Top Narratives — Last Hour</div>', unsafe_allow_html=True)
    df_trends = query("""
        SELECT keyword, COUNT(*) as count
        FROM keyword_index
        WHERE created_at > NOW() - INTERVAL '1 hour'
        GROUP BY keyword
        ORDER BY count DESC
        LIMIT 15
    """)
    if not df_trends.empty:
        max_count = df_trends['count'].max()
        for _, row in df_trends.iterrows():
            pct = int(row['count'] / max_count * 100)
            bar = '█' * int(pct / 5)
            st.markdown(
                f'<div class="trend-item"><span>{row["keyword"].upper()}</span><span>{bar} {row["count"]}</span></div>',
                unsafe_allow_html=True
            )

with col_right:
    st.markdown('<div class="section-title">📈 Token Launch Volume — Last 24H</div>', unsafe_allow_html=True)
    df_volume = query("""
        SELECT 
            DATE_TRUNC('hour', created_at) as hour,
            COUNT(*) as count
        FROM discovered_tokens
        WHERE created_at > NOW() - INTERVAL '24 hours'
        GROUP BY hour
        ORDER BY hour
    """)
    if not df_volume.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_volume['hour'],
            y=df_volume['count'],
            mode='lines+markers',
            line=dict(color='#00ffa3', width=2),
            marker=dict(color='#00ffa3', size=4),
            fill='tozeroy',
            fillcolor='rgba(0,255,163,0.05)'
        ))
        fig.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
            xaxis=dict(gridcolor='rgba(0,255,163,0.08)', showline=False),
            yaxis=dict(gridcolor='rgba(0,255,163,0.08)', showline=False),
            margin=dict(l=0, r=0, t=0, b=0),
            height=380
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── ROW 3: TOP TOKENS BY VOLUME ───────────────────────
st.markdown('<div class="section-title">💰 Top Tokens by Volume — Last 24H</div>', unsafe_allow_html=True)
df_tokens = query("""
    SELECT 
        name, symbol, 
        ROUND(price_usd::numeric, 10) as price,
        ROUND(market_cap::numeric, 0) as mcap,
        ROUND(volume_24h::numeric, 0) as vol_24h,
        buys_24h, sells_24h,
        created_at
    FROM discovered_tokens
    WHERE volume_24h > 0
    AND created_at > NOW() - INTERVAL '24 hours'
    ORDER BY volume_24h DESC
    LIMIT 20
""")

if not df_tokens.empty:
    df_tokens['created_at'] = pd.to_datetime(df_tokens['created_at']).dt.strftime('%H:%M:%S')
    df_tokens.columns = ['Name', 'Symbol', 'Price USD', 'Market Cap', 'Volume 24H', 'Buys', 'Sells', 'Created']
    st.dataframe(
        df_tokens,
        use_container_width=True,
        hide_index=True,
        height=400
    )

st.markdown("<br>", unsafe_allow_html=True)

# ── ROW 4: MARKET CAP DISTRIBUTION ───────────────────
col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section-title">🎯 Market Cap Distribution</div>', unsafe_allow_html=True)
    df_mcap = query("""
        SELECT
            CASE
                WHEN market_cap < 1000 THEN '< $1K'
                WHEN market_cap < 10000 THEN '$1K - $10K'
                WHEN market_cap < 100000 THEN '$10K - $100K'
                WHEN market_cap < 1000000 THEN '$100K - $1M'
                ELSE '> $1M'
            END as range,
            COUNT(*) as count
        FROM discovered_tokens
        WHERE market_cap IS NOT NULL AND market_cap > 0
        GROUP BY range
        ORDER BY MIN(market_cap)
    """)
    if not df_mcap.empty:
        fig2 = go.Figure(go.Bar(
            x=df_mcap['range'],
            y=df_mcap['count'],
            marker_color='rgba(0,255,163,0.7)',
            marker_line_color='rgba(0,255,163,1)',
            marker_line_width=1
        ))
        fig2.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
            xaxis=dict(gridcolor='rgba(0,255,163,0.08)'),
            yaxis=dict(gridcolor='rgba(0,255,163,0.08)'),
            margin=dict(l=0, r=0, t=0, b=0),
            height=300
        )
        st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.markdown('<div class="section-title">⚡ Latest Captures — Live Feed</div>', unsafe_allow_html=True)
    df_live = query("""
        SELECT name, symbol, price_usd, created_at
        FROM discovered_tokens
        WHERE name IS NOT NULL AND name != ''
        ORDER BY created_at DESC
        LIMIT 12
    """)
    if not df_live.empty:
        for _, row in df_live.iterrows():
            name = row['name'] or '[ PENDING METADATA ]'
            symbol = row['symbol'] or '???'
            price = f"${float(row["price_usd"]):.8f}" if (row["price_usd"] is not None and row["price_usd"] == row["price_usd"]) else "NO PRICE"
            time_str = pd.to_datetime(row['created_at']).strftime('%H:%M:%S')
            st.markdown(
                f'<div class="token-row">⚡ <b>{name}</b> <span style="color:rgba(0,255,163,0.5)">({symbol})</span> — <span class="positive">{price}</span> <span style="float:right;color:rgba(255,255,255,0.3)">{time_str}</span></div>',
                unsafe_allow_html=True
            )

# ══════════════════════════════════════════════════════
# ── TRADING PERFORMANCE PANEL ─────────────────────────
# ══════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="header-title" style="font-size:1.6rem">💹 TRADING PERFORMANCE</div>', unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# ── KPIs de capital ───────────────────────────────────
df_cap = query("SELECT capital, daily_pnl, total_pnl, wins, losses FROM paper_capital LIMIT 1")
df_trades_total = query("SELECT COUNT(*) as n, AVG(pnl_pct) as avg_pnl, MAX(pnl_pct) as best, MIN(pnl_pct) as worst FROM paper_trades WHERE status='CLOSED'")

if not df_cap.empty and not df_trades_total.empty:
    cap   = df_cap.iloc[0]
    stats = df_trades_total.iloc[0]
    total_trades = int((cap['wins'] or 0) + (cap['losses'] or 0))
    win_rate = float(cap['wins']) / total_trades * 100 if total_trades > 0 else 0
    roi = (float(cap['capital']) - 1000) / 1000 * 100

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        st.markdown(f'<div class="metric-card"><div class="metric-value">${float(cap["capital"]):,.0f}</div><div class="metric-label">Capital</div></div>', unsafe_allow_html=True)
    with k2:
        color = "#00ffa3" if roi >= 0 else "#ff4466"
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:{color}">{roi:+.1f}%</div><div class="metric-label">ROI Total</div></div>', unsafe_allow_html=True)
    with k3:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{win_rate:.0f}%</div><div class="metric-label">Win Rate</div></div>', unsafe_allow_html=True)
    with k4:
        st.markdown(f'<div class="metric-card"><div class="metric-value">{total_trades}</div><div class="metric-label">Trades</div></div>', unsafe_allow_html=True)
    with k5:
        best = float(stats['best'] or 0)
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#00ffa3">{best:+.0f}%</div><div class="metric-label">Best Trade</div></div>', unsafe_allow_html=True)
    with k6:
        worst = float(stats['worst'] or 0)
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#ff4466">{worst:+.0f}%</div><div class="metric-label">Worst Trade</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── RISK & REGIME DASHBOARD ───────────────────────────
st.markdown('<div class="section-title">🛡️ Risk & Regime</div>', unsafe_allow_html=True)

df_regime = query("""
    SELECT
        COUNT(*) FILTER (WHERE pnl > 0)  AS wins,
        COUNT(*) FILTER (WHERE pnl <= 0) AS losses
    FROM paper_trades
    WHERE status = 'CLOSED' AND closed_at > NOW() - INTERVAL '20 trades'
      AND id IN (SELECT id FROM paper_trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT 20)
""")

df_regime_wr = query("""
    SELECT
        ROUND(COUNT(*) FILTER (WHERE pnl > 0) * 100.0 / NULLIF(COUNT(*), 0), 1) AS wr_20
    FROM (SELECT pnl FROM paper_trades WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT 20) t
""")

df_wr7   = query("SELECT ROUND(COUNT(*) FILTER (WHERE pnl > 0)*100.0/NULLIF(COUNT(*),0),1) AS wr FROM paper_trades WHERE status='CLOSED' AND closed_at > NOW()-INTERVAL '7 days'")
df_wr30  = query("SELECT ROUND(COUNT(*) FILTER (WHERE pnl > 0)*100.0/NULLIF(COUNT(*),0),1) AS wr FROM paper_trades WHERE status='CLOSED' AND closed_at > NOW()-INTERVAL '30 days'")
df_scap  = query("SELECT strategy, capital, initial_capital, COALESCE(peak_capital, initial_capital) as peak FROM strategy_capital")

r1, r2, r3, r4 = st.columns(4)

with r1:
    wr20 = float(df_regime_wr.iloc[0]['wr_20'] or 0) if not df_regime_wr.empty else 0
    if wr20 >= 55:
        regime, rcolor = "RISK ON", "#00ffa3"
    elif wr20 >= 40:
        regime, rcolor = "NEUTRAL", "#ffaa00"
    else:
        regime, rcolor = "RISK OFF", "#ff4466"
    st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:{rcolor};font-size:1.4rem">{regime}</div><div class="metric-label">Régimen (WR20)</div></div>', unsafe_allow_html=True)

with r2:
    if not df_scap.empty:
        total_cap  = float(df_scap['capital'].sum())
        total_peak = float(df_scap['peak'].sum())
        dd_now = (total_peak - total_cap) / total_peak * 100 if total_peak > 0 else 0
        dd_color = "#00ffa3" if dd_now < 5 else ("#ffaa00" if dd_now < 10 else "#ff4466")
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:{dd_color}">{dd_now:.1f}%</div><div class="metric-label">Drawdown desde Peak</div></div>', unsafe_allow_html=True)

with r3:
    wr7 = float(df_wr7.iloc[0]['wr'] or 0) if not df_wr7.empty else 0
    c7  = "#00ffa3" if wr7 >= 50 else "#ff4466"
    st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:{c7}">{wr7:.0f}%</div><div class="metric-label">Win Rate 7d</div></div>', unsafe_allow_html=True)

with r4:
    wr30 = float(df_wr30.iloc[0]['wr'] or 0) if not df_wr30.empty else 0
    c30  = "#00ffa3" if wr30 >= 50 else "#ff4466"
    st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:{c30}">{wr30:.0f}%</div><div class="metric-label">Win Rate 30d</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Heatmap hora × día de semana ──────────────────────
st.markdown('<div class="section-title">🗓️ Heatmap Win Rate — Hora UTC × Día</div>', unsafe_allow_html=True)
df_heat = query("""
    SELECT
        EXTRACT(DOW  FROM opened_at AT TIME ZONE 'UTC') AS dow,
        EXTRACT(HOUR FROM opened_at AT TIME ZONE 'UTC') AS hour_utc,
        COUNT(*) AS trades,
        ROUND(COUNT(*) FILTER (WHERE pnl > 0) * 100.0 / NULLIF(COUNT(*), 0), 1) AS win_rate
    FROM paper_trades
    WHERE status = 'CLOSED' AND opened_at IS NOT NULL
    GROUP BY dow, hour_utc
    HAVING COUNT(*) >= 2
    ORDER BY dow, hour_utc
""")
if not df_heat.empty:
    df_heat['dow']      = df_heat['dow'].astype(int)
    df_heat['hour_utc'] = df_heat['hour_utc'].astype(int)
    df_heat['win_rate'] = df_heat['win_rate'].astype(float)
    DAY_NAMES = {0:'Dom', 1:'Lun', 2:'Mar', 3:'Mié', 4:'Jue', 5:'Vie', 6:'Sáb'}
    df_heat['day_name'] = df_heat['dow'].map(DAY_NAMES)
    pivot = df_heat.pivot_table(index='day_name', columns='hour_utc',
                                values='win_rate', aggfunc='mean')
    day_order = ['Lun','Mar','Mié','Jue','Vie','Sáb','Dom']
    pivot = pivot.reindex([d for d in day_order if d in pivot.index])
    fig_heat = go.Figure(go.Heatmap(
        z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
        colorscale=[[0,'rgba(255,68,102,0.8)'], [0.5,'rgba(255,170,0,0.8)'],
                    [1,'rgba(0,255,163,0.8)']],
        zmin=0, zmax=100,
        text=[[f"{v:.0f}%" if not pd.isna(v) else "" for v in row] for row in pivot.values],
        texttemplate="%{text}", textfont=dict(size=9),
        showscale=True, colorbar=dict(ticksuffix='%', thickness=12)
    ))
    fig_heat.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
        xaxis=dict(title='Hora UTC', gridcolor='rgba(0,255,163,0.06)'),
        yaxis=dict(gridcolor='rgba(0,255,163,0.06)'),
        margin=dict(l=0, r=0, t=10, b=0), height=220
    )
    st.plotly_chart(fig_heat, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Equity Curve + Drawdown ───────────────────────────
perf_left, perf_right = st.columns([2, 1])

with perf_left:
    st.markdown('<div class="section-title">📈 Equity Curve</div>', unsafe_allow_html=True)
    df_equity = query("""
        SELECT closed_at, pnl,
               SUM(pnl) OVER (ORDER BY closed_at ROWS UNBOUNDED PRECEDING) as cumulative_pnl,
               1000 + SUM(pnl) OVER (ORDER BY closed_at ROWS UNBOUNDED PRECEDING) as equity
        FROM paper_trades
        WHERE status = 'CLOSED' AND closed_at IS NOT NULL
        ORDER BY closed_at
    """)
    if not df_equity.empty:
        # Equity curve
        fig_eq = go.Figure()
        fig_eq.add_trace(go.Scatter(
            x=df_equity['closed_at'], y=df_equity['equity'],
            mode='lines+markers',
            line=dict(color='#00ffa3', width=2),
            marker=dict(color='#00ffa3', size=5),
            fill='tozeroy', fillcolor='rgba(0,255,163,0.04)',
            name='Capital'
        ))
        # Línea de capital inicial
        fig_eq.add_hline(y=1000, line_dash='dot', line_color='rgba(255,255,255,0.2)')
        fig_eq.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
            xaxis=dict(gridcolor='rgba(0,255,163,0.08)'),
            yaxis=dict(gridcolor='rgba(0,255,163,0.08)', tickprefix='$'),
            margin=dict(l=0, r=0, t=0, b=0), height=280, showlegend=False
        )
        st.plotly_chart(fig_eq, use_container_width=True)

        # Drawdown
        df_equity['peak']     = df_equity['equity'].cummax()
        df_equity['drawdown'] = (df_equity['equity'] - df_equity['peak']) / df_equity['peak'] * 100
        fig_dd = go.Figure()
        fig_dd.add_trace(go.Scatter(
            x=df_equity['closed_at'], y=df_equity['drawdown'],
            mode='lines', fill='tozeroy',
            line=dict(color='#ff4466', width=1),
            fillcolor='rgba(255,68,102,0.15)',
            name='Drawdown'
        ))
        fig_dd.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
            xaxis=dict(gridcolor='rgba(0,255,163,0.08)'),
            yaxis=dict(gridcolor='rgba(0,255,163,0.08)', ticksuffix='%'),
            margin=dict(l=0, r=0, t=0, b=0), height=150, showlegend=False
        )
        st.plotly_chart(fig_dd, use_container_width=True)

with perf_right:
    st.markdown('<div class="section-title">📋 Últimos Trades</div>', unsafe_allow_html=True)
    df_last = query("""
        SELECT symbol, pnl_pct, exit_reason, closed_at
        FROM paper_trades WHERE status='CLOSED'
        ORDER BY closed_at DESC LIMIT 12
    """)
    if not df_last.empty:
        for _, row in df_last.iterrows():
            pct   = float(row['pnl_pct'] or 0)
            color = "#00ffa3" if pct > 0 else "#ff4466"
            sign  = "▲" if pct > 0 else "▼"
            sym   = row['symbol'] or '???'
            reason = str(row['exit_reason'] or '')[:12]
            t     = pd.to_datetime(row['closed_at']).strftime('%m/%d %H:%M')
            st.markdown(
                f'<div class="token-row"><b style="color:{color}">{sign}{abs(pct):.1f}%</b> '
                f'<span>{sym}</span> '
                f'<span style="color:rgba(255,255,255,0.3);float:right">{reason} {t}</span></div>',
                unsafe_allow_html=True
            )

st.markdown("<br>", unsafe_allow_html=True)

# ── Win Rate por Narrativa + Por Hora ─────────────────
narr_col, hour_col = st.columns(2)

with narr_col:
    st.markdown('<div class="section-title">🏷️ Win Rate por Narrativa</div>', unsafe_allow_html=True)
    df_narr = query("""
        SELECT
            COALESCE(narrative, 'OTHER') as narrative,
            COUNT(*) as trades,
            COUNT(CASE WHEN pnl > 0 THEN 1 END) as wins,
            ROUND(COUNT(CASE WHEN pnl > 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*),0), 1) as win_rate,
            ROUND(AVG(pnl_pct)::numeric, 1) as avg_pnl
        FROM paper_trades
        WHERE status = 'CLOSED'
        GROUP BY narrative
        HAVING COUNT(*) >= 1
        ORDER BY trades DESC
    """)
    if not df_narr.empty:
        fig_narr = go.Figure(go.Bar(
            x=df_narr['narrative'],
            y=df_narr['win_rate'],
            marker_color=[
                'rgba(0,255,163,0.8)' if v >= 50 else 'rgba(255,68,102,0.8)'
                for v in df_narr['win_rate']
            ],
            text=[f"{v:.0f}%" for v in df_narr['win_rate']],
            textposition='auto',
        ))
        fig_narr.add_hline(y=50, line_dash='dot', line_color='rgba(255,255,255,0.2)')
        fig_narr.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
            xaxis=dict(gridcolor='rgba(0,255,163,0.08)'),
            yaxis=dict(gridcolor='rgba(0,255,163,0.08)', ticksuffix='%', range=[0,100]),
            margin=dict(l=0, r=0, t=0, b=0), height=280
        )
        st.plotly_chart(fig_narr, use_container_width=True)

with hour_col:
    st.markdown('<div class="section-title">⏰ Win Rate por Hora UTC</div>', unsafe_allow_html=True)
    df_hour = query("""
        SELECT
            EXTRACT(HOUR FROM opened_at AT TIME ZONE 'UTC') as hour_utc,
            COUNT(*) as trades,
            ROUND(COUNT(CASE WHEN pnl > 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*),0), 1) as win_rate,
            ROUND(AVG(pnl_pct)::numeric, 1) as avg_pnl
        FROM paper_trades
        WHERE status = 'CLOSED'
        GROUP BY hour_utc
        ORDER BY hour_utc
    """)
    if not df_hour.empty:
        fig_hour = go.Figure(go.Bar(
            x=df_hour['hour_utc'].astype(int),
            y=df_hour['win_rate'],
            marker_color=[
                'rgba(0,255,163,0.8)' if v >= 50 else 'rgba(255,68,102,0.7)'
                for v in df_hour['win_rate']
            ],
            text=[f"{int(t)}t" for t in df_hour['trades']],
            textposition='outside',
        ))
        fig_hour.add_hline(y=50, line_dash='dot', line_color='rgba(255,255,255,0.2)')
        fig_hour.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
            xaxis=dict(gridcolor='rgba(0,255,163,0.08)', title='Hora UTC'),
            yaxis=dict(gridcolor='rgba(0,255,163,0.08)', ticksuffix='%', range=[0,110]),
            margin=dict(l=0, r=0, t=0, b=0), height=280
        )
        st.plotly_chart(fig_hour, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Distribución PnL + Exit Reasons ───────────────────
dist_col, exit_col = st.columns(2)

with dist_col:
    st.markdown('<div class="section-title">📊 Distribución de PnL</div>', unsafe_allow_html=True)
    df_pnl_dist = query("""
        SELECT pnl_pct FROM paper_trades
        WHERE status = 'CLOSED' AND pnl_pct IS NOT NULL
    """)
    if not df_pnl_dist.empty:
        fig_dist = go.Figure(go.Histogram(
            x=df_pnl_dist['pnl_pct'],
            nbinsx=20,
            marker_color='rgba(0,255,163,0.6)',
            marker_line_color='rgba(0,255,163,1)',
            marker_line_width=1,
        ))
        fig_dist.add_vline(x=0, line_color='rgba(255,255,255,0.3)')
        fig_dist.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
            xaxis=dict(gridcolor='rgba(0,255,163,0.08)', ticksuffix='%'),
            yaxis=dict(gridcolor='rgba(0,255,163,0.08)'),
            margin=dict(l=0, r=0, t=0, b=0), height=250
        )
        st.plotly_chart(fig_dist, use_container_width=True)

with exit_col:
    st.markdown('<div class="section-title">🚪 Salidas por Razón</div>', unsafe_allow_html=True)
    df_exit = query("""
        SELECT exit_reason,
               COUNT(*) as trades,
               ROUND(AVG(pnl_pct)::numeric, 1) as avg_pnl,
               COUNT(CASE WHEN pnl > 0 THEN 1 END) as wins
        FROM paper_trades WHERE status='CLOSED' AND exit_reason IS NOT NULL
        GROUP BY exit_reason ORDER BY trades DESC
    """)
    if not df_exit.empty:
        colors = {
            'TAKE_PROFIT':   'rgba(0,255,163,0.8)',
            'TRAILING_STOP': 'rgba(0,200,255,0.8)',
            'TIMEOUT':       'rgba(255,170,0,0.8)',
            'STOP_LOSS':     'rgba(255,68,102,0.8)',
        }
        fig_exit = go.Figure(go.Bar(
            x=df_exit['exit_reason'],
            y=df_exit['trades'],
            marker_color=[colors.get(r, 'rgba(128,128,128,0.6)') for r in df_exit['exit_reason']],
            text=[f"avg {v:+.0f}%" for v in df_exit['avg_pnl']],
            textposition='auto',
        ))
        fig_exit.update_layout(
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family='Share Tech Mono', color='rgba(0,255,163,0.6)', size=10),
            xaxis=dict(gridcolor='rgba(0,255,163,0.08)'),
            yaxis=dict(gridcolor='rgba(0,255,163,0.08)'),
            margin=dict(l=0, r=0, t=0, b=0), height=250
        )
        st.plotly_chart(fig_exit, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Posiciones abiertas ───────────────────────────────
st.markdown('<div class="section-title">🔓 Posiciones Abiertas</div>', unsafe_allow_html=True)
df_open = query("""
    SELECT symbol, name, entry_price, peak_price, ml_probability,
           survival_score, narrative, stop_loss_pct,
           opened_at,
           ROUND(EXTRACT(EPOCH FROM (NOW() - opened_at)) / 60) as minutes_open
    FROM paper_trades WHERE status = 'OPEN'
    ORDER BY opened_at ASC
""")
if df_open.empty:
    st.markdown('<div class="token-row" style="text-align:center;color:rgba(255,255,255,0.3)">Sin posiciones abiertas</div>', unsafe_allow_html=True)
else:
    for _, row in df_open.iterrows():
        sym   = row['symbol'] or '???'
        name  = row['name'] or sym
        ep    = float(row['entry_price'])
        pk    = float(row['peak_price'] or ep)
        ml    = row['ml_probability'] or 0
        mins  = int(row['minutes_open'] or 0)
        stop  = float(row['stop_loss_pct'] or 0.70)
        gain_peak = (pk - ep) / ep * 100
        st.markdown(
            f'<div class="token-row">'
            f'<b style="color:#00ffa3">{sym}</b> <span style="color:rgba(255,255,255,0.5)">({name[:20]})</span> — '
            f'Entry: ${ep:.8f} | Peak: <span style="color:#ffaa00">{gain_peak:+.1f}%</span> | '
            f'ML: {ml:.0f}% | Stop: -{(1-stop)*100:.0f}% | '
            f'<span style="color:rgba(255,255,255,0.4)">{mins}min</span>'
            f'</div>',
            unsafe_allow_html=True
        )

# ── Backtest histórico ────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="section-title">🔬 Historial de Backtests</div>', unsafe_allow_html=True)
df_bt = query("""
    SELECT run_at, n_trades, win_rate, roi_pct, avg_pnl_pct, best_pct, worst_pct, filter_mode
    FROM backtest_results ORDER BY run_at DESC LIMIT 10
""")
if not df_bt.empty:
    df_bt['run_at'] = pd.to_datetime(df_bt['run_at']).dt.strftime('%Y-%m-%d %H:%M')
    df_bt.columns = ['Fecha', 'Trades', 'Win Rate%', 'ROI%', 'Avg PnL%', 'Best%', 'Worst%', 'Filtros']
    st.dataframe(df_bt, use_container_width=True, hide_index=True)

# ── FOOTER ───────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    f'<div style="text-align:center;font-family:Share Tech Mono;font-size:0.7rem;color:rgba(0,255,163,0.2);letter-spacing:3px">SOLANA INTELLIGENCE PLATFORM v2.0 — BUILT BY DOUGLAS ALVAREZ — {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} UTC</div>',
    unsafe_allow_html=True
)

