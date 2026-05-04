import os
import requests
import logging
import psycopg2
import psycopg2.pool
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

log = logging.getLogger(__name__)

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_message(text, parse_mode="HTML"):
    """Envía mensaje a Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        log.warning(f"Telegram error: {e}")
        return False

def alert_enter(token_name, symbol, mint, score, ml_prob, momentum,
                price, change_pct, buys, sells, narrative, mcap):
    """Alerta de señal ENTER"""
    dex_url = f"https://dexscreener.com/solana/{mint}"
    pump_url = f"https://pump.fun/{mint}"

    stars = "⭐" * min(5, int((ml_prob or 0) / 20))

    msg = (
        f"🚨 <b>ENTER SIGNAL</b> {stars}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{token_name or mint[:8]}</b> ({symbol or '?'})\n"
        f"📊 Narrativa: <b>{narrative or 'OTHER'}</b>\n\n"
        f"🤖 ML Prob:    <b>{ml_prob or 0:.1f}%</b>\n"
        f"💪 Momentum:   <b>{momentum}</b>\n"
        f"🏆 Score:      <b>{score}</b>\n\n"
        f"💰 Precio:     <b>${float(price):.8f}</b>\n"
        f"📈 Cambio:     <b>{change_pct:+.1f}%</b>\n"
        f"💎 MCap:       <b>${float(mcap or 0):,.0f}</b>\n"
        f"🟢 Buys 5m:    <b>{buys}</b>\n"
        f"🔴 Sells 5m:   <b>{sells}</b>\n\n"
        f"🔗 <a href='{dex_url}'>DexScreener</a> | "
        f"<a href='{pump_url}'>Pump.fun</a>"
    )
    return send_message(msg)

def alert_wallet(wallet, mint, token_name, action, mcap, signature):
    """Alerta de smart wallet comprando"""
    dex_url   = f"https://dexscreener.com/solana/{mint}"
    pump_url  = f"https://pump.fun/{mint}"
    solscan   = f"https://solscan.io/tx/{signature}"

    msg = (
        f"🧠 <b>SMART WALLET</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👛 Wallet: <code>{wallet[:8]}...{wallet[-4:]}</code>\n"
        f"⚡ Acción: <b>{action}</b>\n"
        f"🪙 Token: <b>{token_name or mint[:8]}</b>\n"
        f"💎 MCap:  <b>${float(mcap or 0):,.0f}</b>\n\n"
        f"🔗 <a href='{dex_url}'>DexScreener</a> | "
        f"<a href='{pump_url}'>Pump.fun</a> | "
        f"<a href='{solscan}'>TxHash</a>"
    )
    return send_message(msg)

def alert_paper_trade(action, token_name, mint, price, pnl=None,
                      pnl_pct=None, reason=None):
    """Alerta de paper trade"""
    if action == "OPEN":
        msg = (
            f"📝 <b>PAPER TRADE ABIERTO</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 {token_name or mint[:8]}\n"
            f"💰 Entrada: <b>${float(price):.8f}</b>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
    else:
        emoji = "✅" if (pnl or 0) > 0 else "❌"
        msg = (
            f"{emoji} <b>PAPER TRADE CERRADO</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 {token_name or mint[:8]}\n"
            f"💰 P&L: <b>${pnl:+.2f}</b> ({pnl_pct:+.1f}%)\n"
            f"📌 Razón: <b>{reason}</b>\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
    return send_message(msg)

def alert_daily_summary(capital, pnl, pnl_pct, wins, losses, best_trade):
    """Resumen diario"""
    emoji = "📈" if pnl > 0 else "📉"
    msg = (
        f"{emoji} <b>RESUMEN DEL DÍA</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Capital:  <b>${capital:.2f}</b>\n"
        f"📊 P&L:      <b>${pnl:+.2f}</b> ({pnl_pct:+.1f}%)\n"
        f"✅ Wins:     <b>{wins}</b>\n"
        f"❌ Losses:   <b>{losses}</b>\n"
        f"🏆 Mejor:    <b>{best_trade}</b>\n"
        f"📅 {datetime.now().strftime('%Y-%m-%d')}"
    )
    return send_message(msg)

def alert_system_status(status, message):
    """Alerta de estado del sistema"""
    emoji = "✅" if status == "OK" else "⚠️" if status == "WARN" else "🚨"
    msg = f"{emoji} <b>SISTEMA</b>: {message}"
    return send_message(msg)

def alert_missed_summary(missed_rows):
    """
    Resumen de oportunidades perdidas del día anterior.
    missed_rows: lista de (total, tracked, avg_pnl_1h, phantom_total,
                            would_wins, best_pct, best_name, reason)
    """
    if not missed_rows:
        return

    total_missed   = sum(r[0] for r in missed_rows)
    total_tracked  = sum(r[1] for r in missed_rows)
    phantom_total  = sum(float(r[3] or 0) for r in missed_rows)
    total_would_w  = sum(r[4] for r in missed_rows if r[4])

    wr_phantom = (total_would_w / total_tracked * 100) if total_tracked > 0 else 0
    emoji = "📈" if phantom_total > 0 else "📉"

    lines = [
        f"🔍 <b>OPORTUNIDADES PERDIDAS (ayer)</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"❌ Rechazados:   <b>{total_missed}</b>",
        f"📊 Rastreados:   <b>{total_tracked}</b>",
        f"{emoji} PnL fantasma: <b>${phantom_total:+.2f}</b> (ref $20/trade)",
        f"🎯 WR fantasma:  <b>{wr_phantom:.0f}%</b>",
        f"",
        f"<b>Por razón de rechazo:</b>",
    ]

    REASON_LABELS = {
        'ML_FILTER':          '🤖 ML bajo threshold',
        'RUG_FILTER':         '🛡️  Rug alto',
        'QUALITY_FILTER':     '🚫 Calidad',
        'OUTSIDE_HOURS':      '🌙 Fuera de horario',
        'ML_BELOW_STRATEGY':  '📊 ML bajo estrategia',
        'MAX_OPEN':           '🔒 Max trades abiertos',
    }

    for r in missed_rows[:5]:   # top 5 razones
        total_r, tracked_r, avg_r, phantom_r, would_w, best_pct, best_name, reason = r
        label = REASON_LABELS.get(reason, reason)
        avg_str = f"{float(avg_r):+.1f}%" if avg_r is not None else "N/A"
        phantom_str = f"${float(phantom_r):+.2f}" if phantom_r is not None else "N/A"
        best_str = f"{float(best_pct):+.1f}%" if best_pct is not None else "N/A"
        lines.append(
            f"  {label}: {total_r} | avg={avg_str} | {phantom_str}"
        )
        if best_name and best_pct is not None and float(best_pct) > 20:
            lines.append(f"    ↳ mejor: <b>{best_name}</b> {best_str}")

    lines.append(f"📅 {datetime.now().strftime('%Y-%m-%d')}")
    return send_message("\n".join(lines))
