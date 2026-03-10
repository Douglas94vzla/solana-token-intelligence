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
