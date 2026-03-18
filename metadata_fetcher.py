import requests
import psycopg2
import os
import time
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

# Use configured RPC for deployer lookup (Helius), public RPC for Token-2022 metadata
RPC_URL    = os.getenv("RPC_URL")
PUBLIC_RPC = "https://api.mainnet-beta.solana.com"

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"), host=os.getenv("DB_HOST")
    )


def fetch_token2022_uri(mint: str) -> dict | None:
    """
    Reads Token-2022 embedded metadata from the mint account.
    Returns dict with name, symbol, uri or None.
    Uses public Solana RPC — no rate limits.
    """
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getAccountInfo",
            "params": [mint, {"encoding": "jsonParsed"}]
        }
        resp = requests.post(PUBLIC_RPC, json=payload, timeout=10).json()
        value = resp.get("result", {}).get("value")
        if not value:
            return None
        exts = value.get("data", {}).get("parsed", {}).get("info", {}).get("extensions", [])
        for ext in exts:
            if ext.get("extension") == "tokenMetadata":
                state = ext.get("state", {})
                return {
                    "name":   state.get("name"),
                    "symbol": state.get("symbol"),
                    "uri":    state.get("uri", ""),
                }
        return None
    except Exception:
        return None


def fetch_uri_metadata(uri: str) -> dict:
    """
    Fetches the JSON metadata file (IPFS/Arweave/pump.fun CDN).
    Returns dict with description, image, twitter, telegram, website.
    """
    empty = {"description": None, "image_url": None,
             "twitter": None, "telegram": None, "website": None}
    if not uri:
        return empty
    try:
        resp = requests.get(uri, timeout=8)
        if resp.status_code != 200:
            return empty
        data = resp.json()
        return {
            "description": data.get("description"),
            "image_url":   data.get("image"),
            "twitter":     data.get("twitter") or data.get("Twitter"),
            "telegram":    data.get("telegram") or data.get("Telegram"),
            "website":     data.get("website") or data.get("Website"),
        }
    except Exception:
        return empty


def fetch_deployer_helius(mint: str) -> str | None:
    """
    Uses Helius getAsset to get deployer wallet (creator/authority).
    Falls back gracefully if rate-limited.
    """
    try:
        payload = {
            "jsonrpc": "2.0", "id": "my-id",
            "method": "getAsset",
            "params": {
                "id": mint,
                "displayOptions": {"showSystemMetadata": True, "showFungible": True}
            }
        }
        response = requests.post(RPC_URL, json=payload, timeout=10)
        data = response.json()
        if "error" in data:
            return None
        result = data.get("result", {})
        if not result:
            return None

        for creator in result.get("creators", []):
            addr = creator.get("address", "")
            if addr and len(addr) > 30 and not addr.startswith("11111111"):
                return addr
        for auth in result.get("authorities", []):
            if "full" in auth.get("scopes", []):
                addr = auth.get("address", "")
                if addr and len(addr) > 30 and not addr.startswith("11111111"):
                    return addr
        return None
    except Exception:
        return None


def enrich_token(mint: str) -> dict | None:
    """
    Full enrichment pipeline for a single token.
    Returns dict with all fields, or None if name not found.
    """
    # Step 1: Token-2022 embedded metadata (name, symbol, uri)
    t22 = fetch_token2022_uri(mint)
    if not t22 or not t22.get("name"):
        return None

    # Step 2: Fetch URI for social links
    social = fetch_uri_metadata(t22.get("uri", ""))

    # Step 3: Deployer via Helius
    deployer = fetch_deployer_helius(mint)

    return {
        "name":        t22["name"],
        "symbol":      t22["symbol"],
        "description": social["description"],
        "image_url":   social["image_url"],
        "twitter":     social["twitter"],
        "telegram":    social["telegram"],
        "website":     social["website"],
        "deployer":    deployer,
    }


if __name__ == "__main__":
    print("Iniciando Enriquecedor...")
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT mint, created_at FROM discovered_tokens
        WHERE (name IS NULL OR name = '')
        AND mint LIKE '%pump'
        AND created_at < NOW() - INTERVAL '5 minutes'
        AND (fetch_attempts IS NULL OR fetch_attempts < 5)
        ORDER BY fetch_attempts ASC NULLS FIRST, created_at ASC
        LIMIT 500
    """)
    tokens = cur.fetchall()
    print(f"Tokens pendientes: {len(tokens)}")

    success_count = 0
    for mint, t_created in tokens:
        result = enrich_token(mint)
        if result and result["name"]:
            print(
                f"OK: {result['name']} ({result['symbol']})"
                + (f" | deployer: {result['deployer'][:12]}..." if result["deployer"] else "")
                + (f" | twitter: {result['twitter']}" if result["twitter"] else "")
                + (f" | tg: {result['telegram']}" if result["telegram"] else "")
            )
            cur.execute(
                """UPDATE discovered_tokens
                   SET name        = %s,
                       symbol      = %s,
                       description = COALESCE(description, %s),
                       image_url   = COALESCE(image_url, %s),
                       twitter     = COALESCE(twitter, %s),
                       telegram    = COALESCE(telegram, %s),
                       website     = COALESCE(website, %s),
                       deployer_wallet = COALESCE(deployer_wallet, %s),
                       fetch_attempts  = COALESCE(fetch_attempts, 0) + 1
                   WHERE mint = %s""",
                (
                    result["name"], result["symbol"],
                    result["description"], result["image_url"],
                    result["twitter"], result["telegram"], result["website"],
                    result["deployer"], mint
                )
            )
            success_count += 1
        else:
            cur.execute(
                "UPDATE discovered_tokens SET fetch_attempts=COALESCE(fetch_attempts,0)+1 WHERE mint=%s",
                (mint,)
            )
        conn.commit()
        time.sleep(0.1)

    cur.close()
    conn.close()
    print(f"Ciclo terminado. Enriquecidos: {success_count}/{len(tokens)}")
