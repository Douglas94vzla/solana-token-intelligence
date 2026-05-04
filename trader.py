#!/usr/bin/env python3
"""
trader.py — Ejecución real de trades vía Jupiter API + Solana RPC.

Lee las mismas señales ENTER que paper_trading.py y ejecuta swaps reales.
Corre en paralelo con paper trading para medir: latencia, slippage, PnL real vs simulado.

Env vars:
  WALLET_PRIVATE_KEY     Clave privada en base58 (estándar Solana/Phantom)
  TRADE_SIZE_SOL         SOL por trade (default: 0.05)
  LIVE_TRADING_ENABLED   true para enviar transacciones reales (default: false = dry run)
  MAX_LIVE_TRADES        Máximo trades simultáneos (default: 1)
  MAX_SLIPPAGE_BPS       Slippage máximo aceptable en bps (default: 300 = 3%)
  MAX_PRICE_IMPACT_PCT   Price impact máximo de Jupiter para abrir (default: 8%)
"""

import os
import sys
import time
import base64
import base58
import logging
import requests
import psycopg2
import psycopg2.pool
from datetime import datetime, timedelta
from dotenv import load_dotenv

from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

load_dotenv('/root/solana_bot/.env')

# ── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler()]   # systemd redirige stdout → trader.log
)
log = logging.getLogger(__name__)

# ── CONFIG ───────────────────────────────────────────────────────────────────
RPC_URL              = os.getenv('RPC_URL')
WALLET_PRIVATE_KEY   = os.getenv('WALLET_PRIVATE_KEY', '')
TRADE_SIZE_SOL       = float(os.getenv('TRADE_SIZE_SOL', '0.05'))
LIVE_ENABLED         = os.getenv('LIVE_TRADING_ENABLED', 'false').lower() == 'true'
MAX_LIVE_TRADES      = int(os.getenv('MAX_LIVE_TRADES', '1'))
MAX_SLIPPAGE_BPS     = int(os.getenv('MAX_SLIPPAGE_BPS', '300'))
MAX_PRICE_IMPACT_PCT = float(os.getenv('MAX_PRICE_IMPACT_PCT', '8.0'))

# Mismos umbrales que paper_trading.py para comparación directa
ML_MIN              = 80
TAKE_PROFIT         = 1.80   # +80%
STOP_LOSS           = 0.70   # -30%
MAX_HOLD_MINUTES    = 30
FEES_PCT            = 0.005  # 0.5% estimado (Jupiter cobra ~0.3% + gas)

SOL_MINT            = 'So11111111111111111111111111111111111111112'
JUPITER_QUOTE_URL   = 'https://api.jup.ag/swap/v1/quote'
JUPITER_SWAP_URL    = 'https://api.jup.ag/swap/v1/swap'
TRADE_HOURS_UTC     = (13, 23)
POSITION_INTERVAL   = 10   # segundos entre revisiones de posiciones
SIGNAL_INTERVAL     = 60   # segundos entre búsqueda de señales

# ── DB POOL ──────────────────────────────────────────────────────────────────
pool = psycopg2.pool.ThreadedConnectionPool(
    1, 3,
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST', 'localhost')
)

# ── DB SETUP ─────────────────────────────────────────────────────────────────
def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS live_trades (
                id                  SERIAL PRIMARY KEY,
                mint                TEXT NOT NULL,
                name                TEXT,
                symbol              TEXT,

                -- Compra
                sol_in              NUMERIC(18,9),
                token_amount        NUMERIC(18,9),   -- UI amount (después de decimales)
                token_amount_raw    BIGINT,          -- lamports del token (para sells)
                buy_price_usd       NUMERIC(18,9),   -- fill real: (sol_in * sol_price) / tokens
                paper_price_usd     NUMERIC(18,9),   -- precio DexScreener en el mismo momento
                buy_slippage_bps    INTEGER,         -- (fill - paper) / paper * 10000
                buy_tx              TEXT,
                buy_price_impact    NUMERIC(6,3),    -- price impact Jupiter en %

                -- Venta
                sell_price_usd      NUMERIC(18,9),
                sol_out             NUMERIC(18,9),
                sell_tx             TEXT,
                sell_slippage_bps   INTEGER,
                sell_price_impact   NUMERIC(6,3),

                -- PnL
                pnl_sol             NUMERIC(18,9),
                pnl_pct             NUMERIC(10,4),

                -- Métricas de latencia (ms)
                latency_signal_quote_ms     INTEGER,  -- señal → quote Jupiter
                latency_quote_submit_ms     INTEGER,  -- quote → tx enviada RPC
                latency_submit_confirm_ms   INTEGER,  -- tx enviada → confirmada on-chain
                latency_total_ms            INTEGER,  -- señal → confirmada

                -- Control
                ml_probability      NUMERIC(5,2),
                survival_score      INTEGER,
                narrative           TEXT,
                peak_price_usd      NUMERIC(18,9),
                stop_loss_pct       NUMERIC(5,4),
                status              TEXT DEFAULT 'OPEN',
                exit_reason         TEXT,
                signal_at           TIMESTAMP,
                quote_at            TIMESTAMP,
                tx_submitted_at     TIMESTAMP,
                confirmed_at        TIMESTAMP,
                opened_at           TIMESTAMP DEFAULT NOW(),
                closed_at           TIMESTAMP,
                dry_run             BOOLEAN DEFAULT TRUE,
                error               TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS live_trades_status ON live_trades(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS live_trades_opened ON live_trades(opened_at)")
        conn.commit()
        cur.close()
        log.info("✅ Tabla live_trades lista")
    finally:
        pool.putconn(conn)

# ── WALLET ───────────────────────────────────────────────────────────────────
def load_wallet() -> Keypair | None:
    if not WALLET_PRIVATE_KEY:
        log.warning("⚠️  WALLET_PRIVATE_KEY no configurada — dry run forzado")
        return None
    try:
        key_bytes = base58.b58decode(WALLET_PRIVATE_KEY)
        kp = Keypair.from_bytes(key_bytes)
        log.info(f"🔑 Wallet cargada: {kp.pubkey()}")
        return kp
    except Exception as e:
        log.error(f"❌ Error cargando wallet: {e}")
        return None

# ── SOLANA RPC ───────────────────────────────────────────────────────────────
def rpc(method: str, params: list, timeout: int = 20) -> dict:
    resp = requests.post(RPC_URL, json={
        'jsonrpc': '2.0', 'id': 1,
        'method': method, 'params': params
    }, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def get_sol_balance(pubkey: str) -> float:
    r = rpc('getBalance', [pubkey, {'commitment': 'confirmed'}])
    return r['result']['value'] / 1e9

def get_token_account(wallet: str, mint: str) -> dict | None:
    """Devuelve balance UI, raw amount y decimals del token account del wallet."""
    r = rpc('getTokenAccountsByOwner', [
        wallet,
        {'mint': mint},
        {'encoding': 'jsonParsed', 'commitment': 'confirmed'}
    ])
    accounts = r.get('result', {}).get('value', [])
    if not accounts:
        return None
    info = accounts[0]['account']['data']['parsed']['info']['tokenAmount']
    return {
        'ui':       float(info.get('uiAmount') or 0),
        'raw':      int(info['amount']),
        'decimals': info['decimals'],
    }

def confirm_tx(sig: str, timeout_s: int = 60) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = rpc('getSignatureStatuses', [[sig], {'searchTransactionHistory': True}])
        st = (r.get('result', {}).get('value') or [None])[0]
        if st:
            if st.get('err'):
                log.error(f"❌ Tx falló on-chain: {st['err']}")
                return False
            if st.get('confirmationStatus') in ('confirmed', 'finalized'):
                return True
        time.sleep(2)
    log.error(f"⏱️  Timeout confirmando {sig[:20]}...")
    return False

# ── SOL PRICE (cacheado 60s) ─────────────────────────────────────────────────
_sol_cache: dict = {}

def get_sol_price() -> float:
    if time.time() - _sol_cache.get('t', 0) < 60 and _sol_cache.get('p'):
        return _sol_cache['p']
    for url, extractor in [
        ('https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT',
         lambda r: float(r.json()['price'])),
        ('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd',
         lambda r: float(r.json()['solana']['usd'])),
    ]:
        try:
            r = requests.get(url, timeout=6)
            p = extractor(r)
            _sol_cache.update({'p': p, 't': time.time()})
            return p
        except Exception:
            continue
    return _sol_cache.get('p') or 85.0  # último fallback

# ── DEXSCREENER PRICE ────────────────────────────────────────────────────────
def get_market_price(mint: str) -> float | None:
    try:
        r = requests.get(f'https://api.dexscreener.com/latest/dex/tokens/{mint}', timeout=10)
        pairs = r.json().get('pairs') or []
        if pairs:
            pair = max(pairs, key=lambda p: p.get('volume', {}).get('h24', 0))
            p = pair.get('priceUsd')
            if p:
                return float(p)
    except Exception:
        pass
    try:
        r = requests.get(f'https://api.jup.ag/price/v2?ids={mint}', timeout=8)
        data = r.json().get('data', {}).get(mint)
        if data and data.get('price'):
            return float(data['price'])
    except Exception:
        pass
    return None

# ── JUPITER API ───────────────────────────────────────────────────────────────
def jupiter_quote(input_mint: str, output_mint: str, amount_raw: int) -> dict | None:
    try:
        r = requests.get(JUPITER_QUOTE_URL, params={
            'inputMint':        input_mint,
            'outputMint':       output_mint,
            'amount':           amount_raw,
            'slippageBps':      MAX_SLIPPAGE_BPS,
            'onlyDirectRoutes': 'false',
        }, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"❌ Jupiter quote error: {e}")
        return None

def jupiter_swap_tx(quote: dict, wallet_pubkey: str) -> str | None:
    """Devuelve transacción base64 lista para firmar."""
    try:
        r = requests.post(JUPITER_SWAP_URL, json={
            'quoteResponse':             quote,
            'userPublicKey':             wallet_pubkey,
            'dynamicComputeUnitLimit':   True,
            'prioritizationFeeLamports': 'auto',
            'wrapAndUnwrapSol':          True,
        }, timeout=15)
        r.raise_for_status()
        return r.json().get('swapTransaction')
    except Exception as e:
        log.error(f"❌ Jupiter swap tx error: {e}")
        return None

def sign_and_send(swap_tx_b64: str, keypair: Keypair) -> str | None:
    try:
        raw    = base64.b64decode(swap_tx_b64)
        tx     = VersionedTransaction.from_bytes(raw)
        signed = VersionedTransaction(tx.message, [keypair])
        signed_b64 = base64.b64encode(bytes(signed)).decode()

        r = rpc('sendTransaction', [
            signed_b64,
            {'encoding': 'base64', 'skipPreflight': False,
             'preflightCommitment': 'confirmed', 'maxRetries': 3}
        ])
        if 'error' in r:
            log.error(f"❌ sendTransaction RPC error: {r['error']}")
            return None
        return r.get('result')  # firma de la tx
    except Exception as e:
        log.error(f"❌ sign_and_send error: {e}")
        return None

# ── OPEN TRADE ───────────────────────────────────────────────────────────────
def open_trade(mint: str, name: str, symbol: str, paper_price: float,
               ml_prob: float, score: int, narrative: str,
               keypair: Keypair | None) -> int | None:

    signal_at = datetime.utcnow()
    dry = not LIVE_ENABLED or keypair is None

    # ── Quote de Jupiter (mide latencia aquí incluso en dry run)
    lamports_in = int(TRADE_SIZE_SOL * 1e9)
    quote_at = datetime.utcnow()
    quote = jupiter_quote(SOL_MINT, mint, lamports_in)
    if not quote:
        return None

    price_impact = float(quote.get('priceImpactPct') or 0)
    out_raw      = int(quote['outAmount'])    # tokens raw
    lat_sq = int((datetime.utcnow() - signal_at).total_seconds() * 1000)

    if price_impact > MAX_PRICE_IMPACT_PCT:
        log.warning(f"⚠️  Price impact {price_impact:.1f}% > {MAX_PRICE_IMPACT_PCT}% — skip {symbol}")
        return None

    # ──────────────────────────── DRY RUN ────────────────────────────────────
    if dry:
        sol_price   = get_sol_price()
        # Estimamos fill usando price impact del quote
        fill_price  = paper_price * (1 + price_impact / 100)
        tokens_est  = (TRADE_SIZE_SOL * sol_price) / fill_price if fill_price > 0 else 0
        slip_bps    = int(price_impact * 100)

        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO live_trades
                  (mint, name, symbol, sol_in, token_amount, token_amount_raw,
                   buy_price_usd, paper_price_usd, buy_slippage_bps, buy_tx,
                   buy_price_impact, ml_probability, survival_score, narrative,
                   peak_price_usd, stop_loss_pct, status,
                   signal_at, quote_at, tx_submitted_at, confirmed_at,
                   latency_signal_quote_ms, latency_quote_submit_ms,
                   latency_submit_confirm_ms, latency_total_ms, dry_run)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'DRY_RUN',%s,%s,%s,%s,
                        %s,%s,'OPEN',%s,%s,%s,%s,%s,0,0,%s,TRUE)
                RETURNING id
            """, (mint, name, symbol, TRADE_SIZE_SOL, tokens_est, out_raw,
                  fill_price, paper_price, slip_bps, price_impact,
                  ml_prob, score, narrative,
                  fill_price, round(1 - STOP_LOSS, 2),
                  signal_at, quote_at, quote_at, quote_at,
                  lat_sq, lat_sq))
            tid = cur.fetchone()[0]
            conn.commit()
            cur.close()
        finally:
            pool.putconn(conn)

        log.info(f"🔵 [DRY RUN] OPEN #{tid} | {symbol} | "
                 f"fill_est: ${fill_price:.8f} | paper: ${paper_price:.8f} | "
                 f"impact: {price_impact:.2f}% | quote_lat: {lat_sq}ms")
        return tid

    # ──────────────────────────── LIVE ───────────────────────────────────────
    swap_tx = jupiter_swap_tx(quote, str(keypair.pubkey()))
    if not swap_tx:
        return None

    # Balance pre-compra
    pre = get_token_account(str(keypair.pubkey()), mint)
    pre_raw = pre['raw'] if pre else 0

    tx_submitted_at = datetime.utcnow()
    sig = sign_and_send(swap_tx, keypair)
    if not sig:
        return None
    log.info(f"📤 Buy tx enviada: {sig[:20]}...")
    lat_qs = int((datetime.utcnow() - quote_at).total_seconds() * 1000)

    if not confirm_tx(sig, timeout_s=60):
        _save_failed(mint, name, symbol, paper_price, ml_prob,
                     signal_at, quote_at, tx_submitted_at, sig, 'BUY_TIMEOUT')
        return None
    confirmed_at = datetime.utcnow()

    # Balance post-compra → tokens reales recibidos
    post = get_token_account(str(keypair.pubkey()), mint)
    if not post:
        log.error(f"❌ No se encontró token account tras compra de {symbol}")
        return None

    actual_raw    = post['raw'] - pre_raw
    actual_ui     = post['ui'] - (pre['ui'] if pre else 0)
    decimals      = post['decimals']

    sol_price     = get_sol_price()
    fill_price    = (TRADE_SIZE_SOL * sol_price) / actual_ui if actual_ui > 0 else paper_price
    slip_bps      = int(((fill_price - paper_price) / paper_price) * 10000) if paper_price else 0

    lat_sc = int((confirmed_at - tx_submitted_at).total_seconds() * 1000)
    lat_total = int((confirmed_at - signal_at).total_seconds() * 1000)

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO live_trades
              (mint, name, symbol, sol_in, token_amount, token_amount_raw,
               buy_price_usd, paper_price_usd, buy_slippage_bps, buy_tx,
               buy_price_impact, ml_probability, survival_score, narrative,
               peak_price_usd, stop_loss_pct, status,
               signal_at, quote_at, tx_submitted_at, confirmed_at,
               latency_signal_quote_ms, latency_quote_submit_ms,
               latency_submit_confirm_ms, latency_total_ms, dry_run)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
            RETURNING id
        """, (mint, name, symbol, TRADE_SIZE_SOL, actual_ui, actual_raw,
              fill_price, paper_price, slip_bps, sig,
              price_impact, ml_prob, score, narrative,
              fill_price, round(1 - STOP_LOSS, 2),
              signal_at, quote_at, tx_submitted_at, confirmed_at,
              lat_sq, lat_qs, lat_sc, lat_total))
        tid = cur.fetchone()[0]
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

    log.info(f"✅ LIVE OPEN #{tid} | {symbol} | "
             f"fill: ${fill_price:.8f} | paper: ${paper_price:.8f} | "
             f"slip: {slip_bps:+d}bps | latency: {lat_total}ms | tx: {sig[:20]}")
    return tid

def _save_failed(mint, name, symbol, paper_price, ml_prob,
                 signal_at, quote_at, tx_submitted_at, sig, error):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO live_trades
              (mint, name, symbol, paper_price_usd, ml_probability,
               signal_at, quote_at, tx_submitted_at, buy_tx,
               status, error, dry_run)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'FAILED',%s,FALSE)
        """, (mint, name, symbol, paper_price, ml_prob,
              signal_at, quote_at, tx_submitted_at, sig, error))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

# ── CLOSE TRADE ───────────────────────────────────────────────────────────────
def close_trade(trade: dict, reason: str, current_price: float,
                keypair: Keypair | None):

    tid        = trade['id']
    mint       = trade['mint']
    symbol     = trade['symbol'] or mint[:8]
    buy_price  = float(trade['buy_price_usd'])
    sol_in     = float(trade['sol_in'])
    dry        = trade['dry_run'] or not LIVE_ENABLED or keypair is None

    # ──────────────────────────── DRY RUN ────────────────────────────────────
    if dry:
        sell_price = current_price * (1 - FEES_PCT)
        pnl_pct    = (sell_price - buy_price) / buy_price * 100
        pnl_sol    = sol_in * (pnl_pct / 100)

        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE live_trades SET
                    sell_price_usd = %s, sol_out = %s, sell_tx = 'DRY_RUN',
                    sell_slippage_bps = 0, sell_price_impact = 0,
                    pnl_sol = %s, pnl_pct = %s,
                    status = 'CLOSED', exit_reason = %s, closed_at = NOW()
                WHERE id = %s
            """, (sell_price, sol_in + pnl_sol, pnl_sol, pnl_pct, reason, tid))
            conn.commit()
            cur.close()
        finally:
            pool.putconn(conn)

        emoji = '✅' if pnl_pct > 0 else '❌'
        log.info(f"{emoji} [DRY RUN] CLOSE #{tid} | {symbol} | "
                 f"PnL: {pnl_pct:+.1f}% ({pnl_sol:+.5f} SOL) | {reason}")
        return pnl_pct

    # ──────────────────────────── LIVE ───────────────────────────────────────
    # Obtenemos el balance actual del token (por si cambió desde la compra)
    token_info = get_token_account(str(keypair.pubkey()), mint)
    if not token_info or token_info['raw'] == 0:
        log.warning(f"⚠️  Sin tokens de {symbol} en wallet — ya vendido?")
        return None

    token_raw = token_info['raw']
    token_ui  = token_info['ui']

    # Quote sell: token → SOL
    quote = jupiter_quote(mint, SOL_MINT, token_raw)
    if not quote:
        return None

    price_impact = float(quote.get('priceImpactPct') or 0)
    out_lamports = int(quote['outAmount'])

    swap_tx = jupiter_swap_tx(quote, str(keypair.pubkey()))
    if not swap_tx:
        return None

    pre_sol_bal = get_sol_balance(str(keypair.pubkey()))

    sig = sign_and_send(swap_tx, keypair)
    if not sig:
        return None
    log.info(f"📤 Sell tx enviada: {sig[:20]}...")

    if not confirm_tx(sig, timeout_s=60):
        log.error(f"❌ Sell tx no confirmada para #{tid}: {sig[:20]}")
        return None

    post_sol_bal = get_sol_balance(str(keypair.pubkey()))
    sol_out      = post_sol_bal - pre_sol_bal
    if sol_out <= 0:
        sol_out = out_lamports / 1e9  # fallback al quote

    sol_price      = get_sol_price()
    sell_price_usd = (sol_out * sol_price) / token_ui if token_ui > 0 else current_price
    slip_bps       = int(((sell_price_usd - current_price) / current_price) * 10000) if current_price else 0
    pnl_sol        = sol_out - sol_in
    pnl_pct        = pnl_sol / sol_in * 100

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE live_trades SET
                sell_price_usd = %s, sol_out = %s, sell_tx = %s,
                sell_slippage_bps = %s, sell_price_impact = %s,
                pnl_sol = %s, pnl_pct = %s,
                status = 'CLOSED', exit_reason = %s, closed_at = NOW()
            WHERE id = %s
        """, (sell_price_usd, sol_out, sig, slip_bps, price_impact,
              pnl_sol, pnl_pct, reason, tid))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

    emoji = '✅' if pnl_pct > 0 else '❌'
    log.info(f"{emoji} LIVE CLOSE #{tid} | {symbol} | "
             f"fill: ${sell_price_usd:.8f} (slip: {slip_bps:+d}bps, impact: {price_impact:.2f}%) | "
             f"PnL: {pnl_pct:+.1f}% ({pnl_sol:+.5f} SOL) | {reason} | tx: {sig[:20]}")
    return pnl_pct

# ── MONITOR POSICIONES ────────────────────────────────────────────────────────
def update_peak(tid: int, peak: float):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE live_trades SET peak_price_usd = %s WHERE id = %s", (peak, tid))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def get_open_trades() -> list[dict]:
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, mint, name, symbol, buy_price_usd, token_amount, token_amount_raw,
                   sol_in, peak_price_usd, stop_loss_pct, opened_at, dry_run
            FROM live_trades WHERE status = 'OPEN'
            ORDER BY opened_at ASC
        """)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def monitor_positions(keypair: Keypair | None):
    for trade in get_open_trades():
        mint     = trade['mint']
        symbol   = trade['symbol'] or mint[:8]
        entry    = float(trade['buy_price_usd'] or 0)
        peak     = float(trade['peak_price_usd'] or entry)
        stop_pct = float(trade['stop_loss_pct'] or round(1 - STOP_LOSS, 2))
        opened   = trade['opened_at'].replace(tzinfo=None)
        age_min  = (datetime.utcnow() - opened).total_seconds() / 60

        current = get_market_price(mint)
        if current is None:
            continue

        if current > peak:
            peak = current
            update_peak(trade['id'], peak)

        reason = None
        if age_min >= MAX_HOLD_MINUTES:
            reason = 'TIMEOUT'
        elif current >= entry * TAKE_PROFIT:
            reason = 'TAKE_PROFIT'
        elif entry > 0 and current <= entry * (1 - stop_pct):
            reason = 'STOP_LOSS'

        if reason:
            close_trade(trade, reason, current, keypair)

# ── BUSCAR SEÑALES ────────────────────────────────────────────────────────────
def check_signals(keypair: Keypair | None):
    hour = datetime.utcnow().hour
    if not (TRADE_HOURS_UTC[0] <= hour < TRADE_HOURS_UTC[1]):
        return

    open_trades = get_open_trades()
    if len(open_trades) >= MAX_LIVE_TRADES:
        return

    # Mints ya en live_trades (abiertas o cerradas recientemente)
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT mint FROM live_trades
            WHERE status = 'OPEN'
               OR (status IN ('CLOSED','FAILED')
                   AND opened_at > NOW() - INTERVAL '2 hours')
        """)
        busy = {r[0] for r in cur.fetchall()}
        cur.close()
    finally:
        pool.putconn(conn)

    # Misma query que paper_trading.check_new_signals()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT dt.mint, dt.name, dt.symbol, dt.price_usd,
                   dt.ml_probability, dt.survival_score, dt.narrative
            FROM discovered_tokens dt
            WHERE dt.entry_signal = 'ENTER'
              AND dt.ml_probability >= %s
              AND dt.entry_at > NOW() - INTERVAL '30 minutes'
              AND dt.market_cap >= 5000
              AND (dt.pair_address IS NULL OR dt.liquidity_usd >= 5000)
              AND (dt.rug_flags IS NULL OR dt.rug_flags NOT LIKE '%%NEAR_ZERO_LIQUIDITY%%')
              AND (dt.price_change_1h IS NULL OR dt.price_change_1h > -70)
            ORDER BY dt.ml_probability DESC
            LIMIT 5
        """, (ML_MIN,))
        signals = cur.fetchall()
        cur.close()
    finally:
        pool.putconn(conn)

    for mint, name, symbol, paper_price, ml_prob, score, narrative in signals:
        if mint in busy:
            continue
        if len(get_open_trades()) >= MAX_LIVE_TRADES:
            break
        if not paper_price:
            continue

        log.info(f"🎯 ENTER detectado: {symbol} | ML: {ml_prob}% | ${float(paper_price):.8f}")
        open_trade(mint, name, symbol, float(paper_price),
                   float(ml_prob), score or 0, narrative or '', keypair)

# ── REPORTE DE COMPARACIÓN ────────────────────────────────────────────────────
def comparison_report():
    conn = pool.getconn()
    try:
        cur = conn.cursor()

        # Estadísticas live_trades (24h)
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'CLOSED')                     AS total,
                COUNT(*) FILTER (WHERE status = 'CLOSED' AND pnl_pct > 0)     AS wins,
                ROUND(AVG(pnl_pct)      FILTER (WHERE status='CLOSED')::numeric, 2) AS avg_pnl,
                ROUND(AVG(latency_total_ms)::numeric, 0)                       AS avg_lat,
                ROUND(AVG(buy_slippage_bps)::numeric, 0)                       AS avg_buy_slip,
                ROUND(AVG(sell_slippage_bps)::numeric, 0)                      AS avg_sell_slip,
                COUNT(*) FILTER (WHERE dry_run = FALSE AND status = 'CLOSED')  AS live_closed,
                SUM(pnl_sol) FILTER (WHERE dry_run = FALSE AND status='CLOSED') AS live_pnl_sol
            FROM live_trades
            WHERE opened_at > NOW() - INTERVAL '24 hours'
        """)
        r = cur.fetchone()
        total, wins = r[0] or 0, r[1] or 0
        wr = wins / total * 100 if total else 0

        log.info("─" * 60)
        log.info(f"📊 REPORTE 24h | Trades: {total} | WR: {wr:.0f}% | avg PnL: {r[2] or 0:.2f}%")
        log.info(f"   Latencia: {r[3] or 0:.0f}ms | Buy slip: {r[4] or 0:.0f}bps | Sell slip: {r[5] or 0:.0f}bps")
        if r[6]:
            log.info(f"   LIVE ejecutados: {r[6]} | PnL real: {r[7]:+.5f} SOL")

        # Comparación paper vs live en los mismos tokens
        cur.execute("""
            SELECT
                lt.symbol,
                lt.buy_price_usd         AS live_entry,
                pt.entry_price           AS paper_entry,
                lt.buy_slippage_bps,
                lt.latency_total_ms,
                lt.pnl_pct               AS live_pnl,
                pt.pnl_pct               AS paper_pnl
            FROM live_trades lt
            LEFT JOIN paper_trades pt
              ON lt.mint = pt.mint
             AND ABS(EXTRACT(EPOCH FROM (lt.opened_at - pt.opened_at))) < 120
            WHERE lt.opened_at > NOW() - INTERVAL '24 hours'
              AND lt.status = 'CLOSED'
            ORDER BY lt.opened_at DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        if rows:
            log.info("   Symbol           Live entry    Paper entry   Slip    Lat     Live PnL  Paper PnL")
            for row in rows:
                sym, le, pe, slip, lat, lpnl, ppnl = row
                pe_str   = f"${pe:.8f}"  if pe   else "  —  "
                lpnl_str = f"{lpnl:+.1f}%" if lpnl else "  —  "
                ppnl_str = f"{ppnl:+.1f}%" if ppnl else "  —  "
                log.info(f"   {sym:<16} ${le:.8f} {pe_str} {slip or 0:+5d}bps {lat or 0:5d}ms "
                         f"{lpnl_str:>8} {ppnl_str:>8}")
        log.info("─" * 60)
        cur.close()
    except Exception as e:
        log.error(f"comparison_report error: {e}")
    finally:
        pool.putconn(conn)

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    setup_db()

    keypair = load_wallet()
    mode    = 'LIVE' if (LIVE_ENABLED and keypair) else 'DRY RUN'

    log.info("=" * 60)
    log.info(f"🚀 Trader arrancando — modo: {mode}")
    log.info(f"   Trade size: {TRADE_SIZE_SOL} SOL | Max trades: {MAX_LIVE_TRADES}")
    log.info(f"   ML mínimo: {ML_MIN}% | TP: +{int((TAKE_PROFIT-1)*100)}% | SL: -{int((1-STOP_LOSS)*100)}%")
    log.info(f"   Slippage max: {MAX_SLIPPAGE_BPS}bps | Price impact max: {MAX_PRICE_IMPACT_PCT}%")
    if keypair:
        log.info(f"   Wallet: {keypair.pubkey()}")
        if LIVE_ENABLED:
            try:
                bal = get_sol_balance(str(keypair.pubkey()))
                log.info(f"   Balance: {bal:.4f} SOL")
                if bal < TRADE_SIZE_SOL:
                    log.error(f"❌ Balance insuficiente ({bal:.4f} SOL < {TRADE_SIZE_SOL} SOL requerido)")
                    sys.exit(1)
            except Exception as e:
                log.error(f"❌ No se pudo consultar balance: {e}")
    log.info("=" * 60)

    last_signal = 0
    last_report = 0

    while True:
        now = time.time()

        try:
            monitor_positions(keypair)
        except Exception as e:
            log.error(f"monitor_positions error: {e}")

        if now - last_signal >= SIGNAL_INTERVAL:
            try:
                check_signals(keypair)
            except Exception as e:
                log.error(f"check_signals error: {e}")
            last_signal = now

        if now - last_report >= 3600:
            try:
                comparison_report()
            except Exception as e:
                log.error(f"comparison_report error: {e}")
            last_report = now

        time.sleep(POSITION_INTERVAL)

if __name__ == '__main__':
    main()
