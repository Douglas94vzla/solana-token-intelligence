#!/usr/bin/env python3
"""
monitor.py — Health monitor del sistema

Corre cada 5 minutos vía systemd timer. Envía alertas Telegram si:
  - El harvester lleva >10 min sin capturar tokens
  - Cualquier servicio crítico está caído (+ alerta de recuperación)
  - El capital cae más del 5% en el día
  - Hay 3 o más stop loss consecutivos
"""

import os
import json
import logging
import subprocess
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

from telegram_bot import send_message

# ── Configuración ──────────────────────────────────────────────────────────────
LOG_FILE   = '/var/log/solana_bot/monitor.log'
STATE_FILE = '/root/solana_bot/monitor_state.json'

CRITICAL_SERVICES       = ['pump-harvester', 'entry-signal', 'price-fetcher', 'paper-trading']
HARVESTER_SILENCE_MIN   = 10     # minutos sin token → alerta
DAILY_LOSS_THRESHOLD    = 0.05   # 5% de pérdida diaria → alerta
INITIAL_CAPITAL         = 1000.0
CONSECUTIVE_SL_LIMIT    = 3      # N SL seguidos → alerta
ALERT_COOLDOWN_MIN      = 30     # minutos mínimos entre re-alertas del mismo tipo

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler(LOG_FILE)]
)
log = logging.getLogger(__name__)


# ── Estado persistente ─────────────────────────────────────────────────────────
def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)


def cooldown_ok(state: dict, key: str, minutes: int = ALERT_COOLDOWN_MIN) -> bool:
    """True si han pasado suficientes minutos desde la última alerta."""
    last = state.get(key)
    if not last:
        return True
    return datetime.now() - datetime.fromisoformat(last) > timedelta(minutes=minutes)


def mark_alerted(state: dict, key: str):
    state[key] = datetime.now().isoformat()


# ── DB helper ──────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST', 'localhost'),
        connect_timeout=5,
    )


# ── Check 1: Harvester silencioso ──────────────────────────────────────────────
def check_harvester(state: dict):
    key = 'harvester_silent'
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT MAX(created_at) FROM discovered_tokens")
        last_token = cur.fetchone()[0]
        conn.close()

        if last_token is None:
            return

        silence_min = (datetime.now() - last_token.replace(tzinfo=None)).total_seconds() / 60

        if silence_min > HARVESTER_SILENCE_MIN:
            if cooldown_ok(state, key):
                sent = send_message(
                    f"🔇 <b>HARVESTER SILENCIOSO</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"Sin tokens nuevos hace <b>{silence_min:.0f} min</b>\n"
                    f"Último token: <b>{last_token.strftime('%H:%M:%S')}</b>\n"
                    f"⚙️ Revisa: <code>systemctl status pump-harvester</code>"
                )
                if sent:
                    mark_alerted(state, key)
                    log.warning(f"Harvester silencioso: {silence_min:.0f}min")
        else:
            # Limpiar estado cuando se recupera (para poder alertar de nuevo la próxima vez)
            if key in state:
                log.info(f"Harvester activo — limpiando alerta previa")
                state.pop(key, None)

    except Exception as e:
        log.error(f"check_harvester: {e}")


# ── Check 2: Servicios caídos ──────────────────────────────────────────────────
def check_services(state: dict):
    for svc in CRITICAL_SERVICES:
        key_down = f'service_down_{svc}'
        key_was  = f'service_was_down_{svc}'
        try:
            result    = subprocess.run(['systemctl', 'is-active', svc],
                                       capture_output=True, text=True, timeout=5)
            is_active = result.stdout.strip() == 'active'

            if not is_active:
                status = result.stdout.strip()
                state[key_was] = True
                if cooldown_ok(state, key_down):
                    sent = send_message(
                        f"🚨 <b>SERVICIO CAÍDO</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"Servicio: <b>{svc}</b>\n"
                        f"Estado:   <code>{status}</code>\n"
                        f"⏰ {datetime.now().strftime('%H:%M:%S')}\n"
                        f"⚙️ <code>systemctl restart {svc}</code>"
                    )
                    if sent:
                        mark_alerted(state, key_down)
                        log.warning(f"Servicio caído: {svc} ({status})")
            else:
                # Recuperación: sólo alertar si estaba marcado como caído
                if state.pop(key_was, False):
                    send_message(
                        f"✅ <b>SERVICIO RECUPERADO</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"Servicio: <b>{svc}</b> → <b>active</b>\n"
                        f"⏰ {datetime.now().strftime('%H:%M:%S')}"
                    )
                    log.info(f"Servicio recuperado: {svc}")
                state.pop(key_down, None)

        except Exception as e:
            log.error(f"check_services ({svc}): {e}")


# ── Check 3: Caída de capital >5% diario ──────────────────────────────────────
def check_capital(state: dict):
    key = 'capital_loss'
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT capital, daily_pnl FROM paper_capital LIMIT 1")
        row = cur.fetchone()
        conn.close()

        if not row:
            return

        capital, daily_pnl = float(row[0]), float(row[1])
        loss_pct = daily_pnl / INITIAL_CAPITAL  # negativo si es pérdida

        if loss_pct < -DAILY_LOSS_THRESHOLD:
            if cooldown_ok(state, key, minutes=60):   # máx 1 alerta/hora
                sent = send_message(
                    f"📉 <b>PÉRDIDA DIARIA &gt;{DAILY_LOSS_THRESHOLD*100:.0f}%</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"Capital:     <b>${capital:.2f}</b>\n"
                    f"P&L del día: <b>${daily_pnl:+.2f}</b> "
                    f"(<b>{loss_pct*100:+.1f}%</b>)\n"
                    f"Límite:      <b>-{DAILY_LOSS_THRESHOLD*100:.0f}%</b>"
                )
                if sent:
                    mark_alerted(state, key)
                    log.warning(f"Pérdida diaria: {loss_pct*100:.1f}%")
        else:
            state.pop(key, None)

    except Exception as e:
        log.error(f"check_capital: {e}")


# ── Check 4: N stop loss consecutivos ─────────────────────────────────────────
def check_consecutive_sl(state: dict):
    key         = 'consec_sl'
    key_fingerp = 'consec_sl_fingerprint'
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT id, exit_reason, symbol, pnl_pct, closed_at
            FROM paper_trades
            WHERE status = 'CLOSED'
            ORDER BY closed_at DESC
            LIMIT %s
        """, (CONSECUTIVE_SL_LIMIT,))
        trades = cur.fetchall()
        conn.close()

        if len(trades) < CONSECUTIVE_SL_LIMIT:
            return

        all_sl = all(t[1] == 'STOP_LOSS' for t in trades)

        if all_sl:
            # Fingerprint: ID del trade más reciente de la racha
            fingerprint = str(trades[0][0])
            already_alerted = (state.get(key_fingerp) == fingerprint)

            if not already_alerted and cooldown_ok(state, key, minutes=60):
                lines = '\n'.join(
                    f"  • {t[2] or '?'}: {float(t[3]):+.1f}%"
                    for t in reversed(trades)
                )
                sent = send_message(
                    f"🔴 <b>{CONSECUTIVE_SL_LIMIT} STOP LOSS CONSECUTIVOS</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"{lines}\n\n"
                    f"⚠️ Revisar filtros ML o condiciones de mercado"
                )
                if sent:
                    mark_alerted(state, key)
                    state[key_fingerp] = fingerprint
                    log.warning(f"{CONSECUTIVE_SL_LIMIT} SL consecutivos detectados")
        else:
            state.pop(key, None)
            state.pop(key_fingerp, None)

    except Exception as e:
        log.error(f"check_consecutive_sl: {e}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("Monitor check iniciando...")
    state = load_state()

    check_harvester(state)
    check_services(state)
    check_capital(state)
    check_consecutive_sl(state)

    save_state(state)
    log.info("Monitor check completado")


if __name__ == '__main__':
    main()
