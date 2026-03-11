import streamlit as st
import psycopg2
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
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST")
    )

def query(sql, params=None):
    conn = None
    try:
        conn = get_connection()
        result = pd.read_sql(sql, conn, params=params)
        return result
    except Exception as e:
        st.error(f"DB Error: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

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

# ── FOOTER ───────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    f'<div style="text-align:center;font-family:Share Tech Mono;font-size:0.7rem;color:rgba(0,255,163,0.2);letter-spacing:3px">SOLANA INTELLIGENCE PLATFORM v1.0 — BUILT BY DOUGLAS ALVAREZ — {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} UTC</div>',
    unsafe_allow_html=True
)

