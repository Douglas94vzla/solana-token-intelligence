import asyncio
import aiohttp
import json
import websockets
import os
import base64
import base58
import psycopg2
import psycopg2.pool
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/harvester.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

PUMP_FUN_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Endpoints en orden de prioridad
WSS_ENDPOINTS = [
    {"name": "Helius",  "url": os.getenv("WSS_URL_1")},
    {"name": "Ankr",    "url": os.getenv("WSS_URL_2")},
    {"name": "Oficial", "url": os.getenv("WSS_URL_3")},
]

# Backoff por endpoint: intentos fallidos consecutivos → segundos de espera
BACKOFF_SEQUENCE = [30, 60, 120, 240]
PROMOTION_INTERVAL = 1800   # cada 30 min intentar volver al de mayor prioridad
ALL_FAILED_WAIT   = 300     # 5 min si los 3 fallan simultáneamente

seen_mints = set()

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)


# ---------------------------------------------------------------------------
# Estado de rotación de endpoints
# ---------------------------------------------------------------------------

class EndpointManager:
    def __init__(self, endpoints):
        self.endpoints   = endpoints
        self.current_idx = 0
        self.failures    = [0] * len(endpoints)        # fallos consecutivos por endpoint
        self.next_retry  = [0.0] * len(endpoints)      # timestamp UNIX más temprano para reintentar
        self.active_since: datetime | None = None
        self.last_promotion_attempt = 0.0

    def _backoff_for(self, idx: int) -> int:
        """Segundos de espera según número de fallos del endpoint."""
        n = min(self.failures[idx], len(BACKOFF_SEQUENCE) - 1)
        return BACKOFF_SEQUENCE[n]

    def _now(self) -> float:
        return datetime.now(timezone.utc).timestamp()

    def mark_failed(self, idx: int, reason: str):
        """Registra fallo en el endpoint idx y actualiza su backoff."""
        self.failures[idx] += 1
        wait = self._backoff_for(idx)
        self.next_retry[idx] = self._now() + wait
        ep = self.endpoints[idx]["name"]
        log.warning(
            f"⛔ Endpoint [{ep}] falló ({reason}). "
            f"Intento #{self.failures[idx]} — reintento en {wait}s"
        )

    def mark_success(self, idx: int):
        """Resetea el contador de fallos tras conexión exitosa."""
        self.failures[idx] = 0

    def next_available(self) -> int | None:
        """Devuelve el índice del siguiente endpoint disponible (no en backoff).
        Empieza desde el de mayor prioridad."""
        now = self._now()
        for i, ep in enumerate(self.endpoints):
            if now >= self.next_retry[i]:
                return i
        return None  # todos en backoff

    def should_try_promote(self) -> bool:
        """True si han pasado 30 min desde el último intento de promoción."""
        return self._now() - self.last_promotion_attempt >= PROMOTION_INTERVAL

    def try_promote(self) -> int | None:
        """Intenta volver al endpoint de mayor prioridad disponible.
        Devuelve el nuevo índice si cambia, None si ya estamos en el óptimo."""
        self.last_promotion_attempt = self._now()
        best = self.next_available()
        if best is None or best >= self.current_idx:
            return None
        return best

    def switch_to(self, new_idx: int, reason: str):
        """Cambia el endpoint activo y loggea el cambio."""
        old_name = self.endpoints[self.current_idx]["name"]
        new_name = self.endpoints[new_idx]["name"]
        self.current_idx = new_idx
        self.active_since = datetime.now(timezone.utc)
        log.info(
            f"🔄 Cambio de endpoint: [{old_name}] → [{new_name}] | Motivo: {reason} | "
            f"{self.active_since.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        _save_endpoint_status(new_idx, self.endpoints[new_idx], reason, self.active_since)

    @property
    def active_url(self) -> str:
        return self.endpoints[self.current_idx]["url"]

    @property
    def active_name(self) -> str:
        return self.endpoints[self.current_idx]["name"]


ep_manager = EndpointManager(WSS_ENDPOINTS)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def setup_endpoint_table():
    """Crea la tabla de monitoreo de endpoints si no existe."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wss_endpoint_log (
                id            SERIAL PRIMARY KEY,
                endpoint_name TEXT,
                endpoint_url  TEXT,
                activated_at  TIMESTAMPTZ,
                reason        TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wss_status (
                id                   INT PRIMARY KEY DEFAULT 1,
                active_endpoint_name TEXT,
                active_endpoint_url  TEXT,
                active_since         TIMESTAMPTZ,
                updated_at           TIMESTAMPTZ
            )
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        log.error(f"❌ Error creando tablas de endpoint: {e}")
    finally:
        pool.putconn(conn)


def _save_endpoint_status(idx: int, ep: dict, reason: str, activated_at: datetime):
    """Guarda en DB el endpoint activo y registra el cambio en el log."""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        # Log histórico
        cur.execute("""
            INSERT INTO wss_endpoint_log (endpoint_name, endpoint_url, activated_at, reason)
            VALUES (%s, %s, %s, %s)
        """, (ep["name"], ep["url"], activated_at, reason))
        # Estado actual (upsert)
        cur.execute("""
            INSERT INTO wss_status (id, active_endpoint_name, active_endpoint_url, active_since, updated_at)
            VALUES (1, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                active_endpoint_name = EXCLUDED.active_endpoint_name,
                active_endpoint_url  = EXCLUDED.active_endpoint_url,
                active_since         = EXCLUDED.active_since,
                updated_at           = EXCLUDED.updated_at
        """, (ep["name"], ep["url"], activated_at))
        conn.commit()
        cur.close()
    except Exception as e:
        log.warning(f"⚠️ No se pudo guardar estado de endpoint en DB: {e}")
    finally:
        pool.putconn(conn)


# ---------------------------------------------------------------------------
# Price / token helpers (sin cambios funcionales)
# ---------------------------------------------------------------------------

async def fetch_initial_price(mint):
    await asyncio.sleep(10)
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                pairs = data.get("pairs")
                if not pairs:
                    return None
                pair = max(pairs, key=lambda p: p.get("volume", {}).get("h24", 0))
                return {
                    "price_usd":     pair.get("priceUsd"),
                    "market_cap":    pair.get("marketCap"),
                    "volume_24h":    pair.get("volume", {}).get("h24"),
                    "buys_5m":       pair.get("txns", {}).get("m5", {}).get("buys"),
                    "sells_5m":      pair.get("txns", {}).get("m5", {}).get("sells"),
                    "pair_address":  pair.get("pairAddress"),
                    "liquidity_usd": pair.get("liquidity", {}).get("usd"),
                    "fdv":           pair.get("fdv"),
                }
    except Exception as e:
        log.warning(f"Price fetch error para {mint}: {e}")
        return None


async def save_token(mint):
    if mint in seen_mints:
        return
    seen_mints.add(mint)

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO discovered_tokens (mint, status) VALUES (%s, %s) "
            "ON CONFLICT (mint) DO NOTHING RETURNING id",
            (mint, 'new')
        )
        result = cur.fetchone()
        conn.commit()
        cur.close()

        if result:
            log.info(f"🚀 NUEVO TOKEN: {mint}")
            asyncio.create_task(save_initial_price(mint))

    except Exception as e:
        log.error(f"❌ Error DB: {e}")
    finally:
        pool.putconn(conn)


async def save_initial_price(mint):
    price_data = await fetch_initial_price(mint)
    if not price_data:
        return

    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE discovered_tokens SET
                price_usd        = %s,
                market_cap       = %s,
                volume_24h       = %s,
                buys_5m          = %s,
                sells_5m         = %s,
                pair_address     = %s,
                liquidity_usd    = %s,
                fdv              = %s,
                price_updated_at = NOW()
            WHERE mint = %s
        """, (
            price_data["price_usd"], price_data["market_cap"],
            price_data["volume_24h"], price_data["buys_5m"],
            price_data["sells_5m"], price_data["pair_address"],
            price_data["liquidity_usd"], price_data["fdv"], mint
        ))
        conn.commit()
        cur.close()
        log.info(f"💰 Precio inicial guardado: {mint} @ ${price_data['price_usd']}")
    except Exception as e:
        log.error(f"❌ Error guardando precio: {e}")
    finally:
        pool.putconn(conn)


# ---------------------------------------------------------------------------
# Conexión WebSocket con rotación de endpoints
# ---------------------------------------------------------------------------

async def connect_and_listen(idx: int) -> str:
    """Abre la conexión al endpoint idx y escucha hasta que falle.
    Devuelve el motivo del fallo como string."""
    ep = WSS_ENDPOINTS[idx]
    url = ep["url"]
    name = ep["name"]

    try:
        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=30,
            open_timeout=15,
        ) as ws:
            payload = {
                "jsonrpc": "2.0", "id": 1, "method": "logsSubscribe",
                "params": [{"mentions": [PUMP_FUN_ID]}, {"commitment": "processed"}]
            }
            await ws.send(json.dumps(payload))
            ep_manager.mark_success(idx)
            log.info(f"✅ Conectado a [{name}] ({url[:60]}...)")

            # Guardar estado inicial si es la primera conexión
            if ep_manager.active_since is None:
                ep_manager.active_since = datetime.now(timezone.utc)
                _save_endpoint_status(idx, ep, "inicio", ep_manager.active_since)

            while True:
                # Intento de promoción cada 30 min sin interrumpir la escucha
                if ep_manager.should_try_promote():
                    best = ep_manager.try_promote()
                    if best is not None:
                        return f"promoción a [{WSS_ENDPOINTS[best]['name']}] (mayor prioridad)"

                msg = await asyncio.wait_for(ws.recv(), timeout=60)
                data = json.loads(msg)

                if "params" in data:
                    logs = data["params"]["result"]["value"]["logs"]
                    if any("Instruction: Create" in l for l in logs):
                        for log_line in logs:
                            if "Program data:" in log_line:
                                try:
                                    raw_data  = base64.b64decode(log_line.split("Program data: ")[1])
                                    mint_bytes = raw_data[8:40]
                                    mint = base58.b58encode(mint_bytes).decode('utf-8')
                                    if mint.endswith("pump"):
                                        await save_token(mint)
                                except Exception:
                                    continue
                            elif "mint:" in log_line.lower():
                                mint = log_line.split("mint:")[1].split(",")[0].strip()
                                await save_token(mint)

    except websockets.exceptions.InvalidStatus as e:       # websockets >= 14
        return f"HTTP {e.response.status_code}"
    except websockets.exceptions.InvalidStatusCode as e:   # websockets < 14 compat
        return f"HTTP {e.status_code}"
    except (websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK) as e:
        return f"conexión cerrada ({e})"
    except asyncio.TimeoutError:
        return "timeout sin mensajes (60s)"
    except Exception as e:
        return str(e)


async def harvester():
    log.info("📡 Harvester iniciando con rotación de endpoints...")
    setup_endpoint_table()

    while True:
        idx = ep_manager.current_idx

        # Verificar si este endpoint está en backoff
        now = datetime.now(timezone.utc).timestamp()
        if now < ep_manager.next_retry[idx]:
            wait = int(ep_manager.next_retry[idx] - now)
            log.info(f"⏳ [{WSS_ENDPOINTS[idx]['name']}] en backoff, esperando {wait}s...")
            await asyncio.sleep(wait)
            continue

        # Intentar conexión
        reason = await connect_and_listen(idx)
        ep_manager.mark_failed(idx, reason)

        # Buscar siguiente endpoint disponible
        next_idx = ep_manager.next_available()

        if next_idx is None:
            # Todos los endpoints en backoff
            log.error(
                f"🚨 Todos los endpoints fallaron. "
                f"Esperando {ALL_FAILED_WAIT}s antes de reiniciar rotación..."
            )
            await asyncio.sleep(ALL_FAILED_WAIT)
            # Resetear backoffs para forzar reintento desde el primero
            ep_manager.next_retry = [0.0] * len(WSS_ENDPOINTS)
            ep_manager.current_idx = 0
            continue

        if next_idx != idx:
            ep_manager.switch_to(next_idx, reason)
        else:
            # Mismo endpoint pero con backoff aplicado
            wait = int(ep_manager.next_retry[next_idx] - datetime.now(timezone.utc).timestamp())
            if wait > 0:
                log.info(f"⏳ Esperando {wait}s antes de reintentar [{WSS_ENDPOINTS[next_idx]['name']}]...")
                await asyncio.sleep(wait)


if __name__ == "__main__":
    asyncio.run(harvester())
