import requests
import psycopg2
import os
import time
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"), host=os.getenv("DB_HOST")
    )

def fetch_with_helius(mint):
    url = os.getenv("RPC_URL")
    payload = {
        "jsonrpc": "2.0",
        "id": "my-id",
        "method": "getAsset",
        "params": {
            "id": mint,
            "displayOptions": {"showSystemMetadata": True, "showFungible": True}
        }
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        data = response.json()
        if "result" in data and data["result"]:
            result = data["result"]
            content = result.get("content", {})
            metadata = content.get("metadata", {})
            name = metadata.get("name")
            symbol = metadata.get("symbol")
            token_info = result.get("token_info", {})
            symbol = symbol or token_info.get("symbol")
            name = name or result.get("name")
            symbol = symbol or result.get("symbol")
            if name:
                return name, symbol
        return None, None
    except Exception as e:
        print(f"Error RPC: {e}")
        return None, None

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
        name, symbol = fetch_with_helius(mint)
        if name:
            print(f"OK: {name} ({symbol})")
            cur.execute(
                "UPDATE discovered_tokens SET name=%s, symbol=%s, fetch_attempts=COALESCE(fetch_attempts,0)+1 WHERE mint=%s",
                (name, symbol, mint)
            )
            success_count += 1
        else:
            cur.execute(
                "UPDATE discovered_tokens SET fetch_attempts=COALESCE(fetch_attempts,0)+1 WHERE mint=%s",
                (mint,)
            )
        conn.commit()
        time.sleep(0.2)

    cur.close()
    conn.close()
    print(f"Ciclo terminado. Enriquecidos: {success_count}/{len(tokens)}")
