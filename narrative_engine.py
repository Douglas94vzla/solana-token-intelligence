import psycopg2
import psycopg2.pool
import os
import re
import logging
from datetime import datetime
from collections import Counter
from dotenv import load_dotenv

load_dotenv('/root/solana_bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/var/log/solana_bot/narrative_engine.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

pool = psycopg2.pool.ThreadedConnectionPool(
    1, 5,
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST")
)

# ── NARRATIVAS CONOCIDAS ──────────────────────────────
# Orden importa: en caso de empate gana la primera categoría con más matches.
# Keywords más específicos primero para evitar falsos positivos.
NARRATIVE_PATTERNS = {

    # ── TECNOLOGÍA / AI ───────────────────────────────────────────────────
    "AI/AGI": [
        "ai", "agi", "gpt", "neural", "intelligence", "agent", "robot",
        "cyber", "neuro", "brain", "llm", "claude", "openai", "deepmind",
        "grok", "gemini", "copilot", "chatbot", "machine", "learning",
        "algorithm", "compute", "model", "dataset", "siri", "alexa",
        "autopilot", "automate", "synthetic", "digital twin", "sentient",
        "conscious", "singularity", "turing", "hal9000", "skynet",
        "terminator", "matrix", "neo", "bot", "autoresearch",
    ],

    # ── ENERGÍA / PETRÓLEO ────────────────────────────────────────────────
    "OIL/ENERGY": [
        "oil", "petroleum", "crude", "petro", "energy", "gas", "barrel",
        "opec", "fuel", "solar", "nuclear", "drill", "pipeline", "refinery",
        "coal", "fracking", "shale", "natural gas", "lng", "offshore",
    ],

    # ── MEME / ANIMALES CLÁSICOS ──────────────────────────────────────────
    "MEME/DOG": [
        "dog", "inu", "doge", "shib", "cat", "pepe", "wojak", "frog",
        "bonk", "floki", "corgi", "husky", "puppy", "pup", "kitty",
        "kitten", "toad", "chad", "npc", "bobo", "feels", "kek",
        "meme", "degen", "cope", "based", "virgin", "gigachad", "sigma",
        "wif", "bome", "popcat", "grumpycat", "lol", "haha", "bruh",
        "clown", "honk", "reeee", "baka", "ngmi", "wagmi", "lfg",
        "gm", "ser", "fren", "anon", "shill", "aping", "mooning",
        "babydoge", "baby", "chud", "cult", "slop", "bro",
    ],

    # ── POLÍTICA ──────────────────────────────────────────────────────────
    "POLITICS": [
        "trump", "biden", "maga", "elon", "president", "white house",
        "congress", "senate", "election", "democrat", "republican",
        "liberal", "conservative", "vote", "freedom", "liberty",
        "constitution", "patriot", "america", "usa", "border", "wall",
        "tariff", "government", "federal", "putin", "ukraine", "russia",
        "xi", "regime", "coup", "revolution", "protest", "populist",
        "iran", "israel", "epstein", "zelensky", "nato", "sanction",
        "referendum", "impeach", "dictator", "oligarch", "kremlin",
        "pentagon", "cia", "fbi", "nsa", "deep state", "joe",
        "peace", "justice", "democracy", "republic", "socialism",
    ],

    # ── ANIME / JAPÓN ─────────────────────────────────────────────────────
    "ANIME/JAPAN": [
        "anime", "waifu", "kawaii", "japan", "tokyo", "ninja", "samurai",
        "manga", "otaku", "chan", "sensei", "senpai", "naruto", "goku",
        "pokemon", "pikachu", "sailor", "midori", "sakura", "hana",
        "yuki", "shonen", "isekai", "hentai", "tsundere", "chibi",
        "gundam", "evangelion", "bleach", "jujutsu", "demon slayer",
    ],

    # ── DEFI / CRYPTO ─────────────────────────────────────────────────────
    "DEFI/CRYPTO": [
        "defi", "yield", "stake", "liquidity", "protocol", "dao", "vault",
        "swap", "btc", "bitcoin", "eth", "ethereum", "sol", "solana",
        "hodl", "diamond", "hands", "whale", "alpha", "beta",
        "bridge", "chain", "wallet", "token", "coin", "crypto",
        "dex", "cex", "airdrop", "whitelist", "launchpad", "presale",
        "pump", "rug", "bundle", "sniper", "mev", "jito", "raydium",
        "jupiter", "bonding", "curve", "cto", "satoshi", "nakamoto",
        "halving", "fees", "gas", "nonce", "mempool", "validator",
        "xmoney", "cashback", "fund", "lqty", "liquid", "bonded",
    ],

    # ── DEPORTES ──────────────────────────────────────────────────────────
    "SPORTS": [
        "nfl", "nba", "fifa", "football", "basketball", "baseball",
        "championship", "league", "team", "athlete", "coach", "stadium",
        "score", "goal", "touchdown", "slam", "dunk", "homerun",
        "boxing", "ufc", "mma", "wrestling", "tennis", "golf",
        "swimmer", "runner", "marathon", "olympic", "medal",
        "camavinga", "mbappe", "ronaldo", "messi", "neymar",
        "premier", "laliga", "champions", "worldcup", "lebron",
        "jordan", "kobe", "shaq", "brady", "mahomes",
    ],

    # ── CHINA / ASIA ──────────────────────────────────────────────────────
    "CHINA/ASIA": [
        "china", "chinese", "asia", "hong kong", "taiwan", "panda",
        "korea", "korean", "india", "thai", "vietnam", "philippines",
        "biao", "yuan", "renminbi", "alibaba", "tencent", "baidu",
        "sino", "mandarin", "beijing", "shanghai", "silk road",
    ],

    # ── MILITAR / GUERRA ──────────────────────────────────────────────────
    "MILITARY": [
        "military", "war", "army", "navy", "missile", "weapon", "fighter",
        "bomb", "soldier", "tank", "gun", "rifle", "nuclear", "combat",
        "battle", "warrior", "tactical", "marine", "seal", "ranger",
        "sniper", "grenade", "bunker", "general", "colonel", "sergeant",
        "airforce", "drone", "torpedo", "submarine", "warship", "nuke",
        "warfare", "siege", "trench", "frontline", "platoon", "regiment",
    ],

    # ── ESPACIO ───────────────────────────────────────────────────────────
    "SPACE": [
        "space", "nasa", "mars", "rocket", "satellite", "galaxy",
        "asteroid", "solar", "planet", "comet", "meteor", "alien",
        "ufo", "universe", "cosmos", "orbit", "launch", "voyager",
        "black hole", "nebula", "quasar", "aurora", "eclipse",
        "starship", "moonshot", "interstellar", "exoplanet", "hubble",
        "spacex", "iss", "astronaut", "cosmonaut", "sky", "celestial",
        "supernova", "dark matter", "wormhole", "dimension",
    ],

    # ── COMIDA ────────────────────────────────────────────────────────────
    "FOOD": [
        "food", "burger", "pizza", "taco", "sushi", "coffee", "beer",
        "wine", "cake", "bread", "rice", "noodle", "ramen", "chicken",
        "beef", "pork", "shrimp", "lobster", "cheese", "chocolate",
        "candy", "cookie", "donut", "waffle", "pancake", "apple",
        "banana", "lemon", "peach", "cherry", "grape", "reese",
        "cornbread", "snack", "nacho", "burrito", "kebab", "falafel",
        "dumpling", "mochi", "matcha", "espresso", "latte", "brew",
        "whiskey", "vodka", "rum", "tequila", "gin", "bourbon",
    ],

    # ── ABSURDO / NADA ────────────────────────────────────────────────────
    "ABSURD/NIL": [
        "nothing", "shitcoin", "retard", "stupid", "dumb", "useless",
        "worthless", "trash", "garbage", "shit", "fuck", "scam",
        "junk", "idiot", "moron", "loser", "fail", "broken",
        "dead", "zero", "null", "void", "blank", "distorted",
        "weird", "cursed", "random", "insane", "crazy", "lunatic",
        "nonsense", "gibberish", "rip", "yolo", "gg", "ngmi",
        "fatigue", "tired", "slop", "antislop", "cope",
    ],

    # ── RIQUEZA ───────────────────────────────────────────────────────────
    "WEALTH": [
        "trillion", "billion", "millionaire", "trillionaire", "rich",
        "wealth", "lambo", "yacht", "luxury", "gold", "silver",
        "platinum", "profit", "money", "cash", "bag", "greed",
        "invest", "hedge", "fund", "capital", "roi", "gains",
        "tendies", "stacks", "bags", "moon bag", "runner",
    ],

    # ── CISNE NEGRO / CRISIS ──────────────────────────────────────────────
    "BLACK SWAN": [
        "black swan", "blackswan", "chaos", "crash", "collapse",
        "crisis", "recession", "apocalypse", "extinction", "catastrophe",
        "disaster", "doom", "blackout", "meltdown", "breakdown",
        "armageddon", "tribulation", "endgame", "final", "last",
    ],

    # ── IDENTIDAD ─────────────────────────────────────────────────────────
    "IDENTITY": [
        "she", "they", "real", "irl", "human", "person",
        "individual", "self", "identity", "soul", "spirit", "existence",
        "consciousness", "being", "entity", "creature", "guy",
        "man", "woman", "girl", "boy", "kid", "bro", "sis",
    ],

    # ── SALUD / BIO ───────────────────────────────────────────────────────
    "HEALTH/BIO": [
        "health", "bio", "pharma", "vaccine", "virus", "medical",
        "dna", "gene", "doctor", "nurse", "hospital", "cure",
        "medicine", "drug", "pill", "blood", "brain", "heart",
        "cancer", "immune", "antibody", "protein", "cell", "stem",
    ],

    # ── NFT / WEB3 ────────────────────────────────────────────────────────
    "NFT/WEB3": [
        "nft", "ordinal", "inscription", "collection", "pfp",
        "pudgy", "penguins", "bayc", "azuki", "mad lads",
        "metaverse", "web3", "avatar", "generative", "art",
        "opensea", "blur", "tensor", "magic eden", "trait",
        "rarity", "floor", "listed", "mint pass", "allowlist",
        "pedgy", "penguin",
    ],

    # ── CELEBRITIES / MÚSICA ──────────────────────────────────────────────
    "CELEBRITY": [
        "snoop", "eminem", "kanye", "drake", "taylor", "beyonce",
        "rihanna", "bieber", "billie", "doja", "cardi", "nicki",
        "travis", "malone", "tupac", "biggie", "kendrick", "nas",
        "rapper", "celebrity", "actor", "actress", "morgan",
        "lil", "yung", "young", "xxl", "grammy", "oscar", "fame",
        "freddie", "fred", "billy", "rocky", "mirabel", "lulu",
        "epstein", "cyrus", "swift", "weeknd", "khalid",
    ],

    # ── GAMING ────────────────────────────────────────────────────────────
    "GAMING": [
        "game", "gamer", "gaming", "minecraft", "fortnite", "roblox",
        "zelda", "mario", "sonic", "valorant", "dota", "runescape",
        "rpg", "quest", "level", "boss", "spawn", "respawn",
        "noob", "esport", "playstation", "xbox", "nintendo", "steam",
        "twitch", "streamer", "speedrun", "dungeon", "raid", "clan",
        "guild", "server", "map", "skin", "loot", "drop", "grind",
        "leaderboard", "elo", "ranked", "pixel", "retro", "arcade",
    ],

    # ── INTERNET / CULTURA DIGITAL ────────────────────────────────────────
    "INTERNET/CULTURE": [
        "reddit", "twitter", "tiktok", "youtube", "instagram",
        "viral", "trending", "4chan", "discord", "thread",
        "upvote", "karma", "post", "feed", "share", "subscribe",
        "stream", "content", "influencer", "creator", "vibe",
        "views", "tweet", "retweet", "hashtag", "follow",
    ],

    # ── ANIMALES (no perros) ──────────────────────────────────────────────
    "ANIMALS": [
        "bird", "eagle", "hawk", "owl", "wolf", "fox", "lion",
        "tiger", "elephant", "shark", "snake", "gecko", "lizard",
        "horse", "goat", "deer", "penguin", "koala", "kangaroo",
        "platypus", "hippo", "rhino", "gorilla", "monkey", "ape",
        "parrot", "turtle", "crab", "bee", "ant", "spider",
        "butterfly", "squirrel", "hedgehog", "hodgehog", "otter",
        "beaver", "bat", "pigeon", "crow", "raven", "pelican",
        "flamingo", "peacock", "swan", "duck", "goose", "seal",
        "walrus", "dolphin", "octopus", "jellyfish", "starfish",
        "unicorn", "phoenix", "griffin", "hydra",
    ],

    # ── FANTASÍA / MITOLOGÍA ──────────────────────────────────────────────
    "FANTASY/MYTH": [
        "dragon", "wizard", "witch", "magic", "spell", "castle",
        "kingdom", "empire", "realm", "throne", "legend", "hero",
        "sword", "shield", "knight", "elf", "dwarf", "orc",
        "goblin", "demon", "angel", "zeus", "thor", "odin",
        "poseidon", "hades", "medusa", "titan", "olympus",
        "valhalla", "excalibur", "artemis", "apollo", "athena",
        "hermes", "ares", "aphrodite", "pandora", "achilles",
        "odyssey", "iliad", "noctis", "imperium", "glaciarch",
        "arch", "excid", "rend", "king", "queen", "prince",
        "princess", "lord", "dark", "ancient", "mythic", "epic",
        "lore", "saga", "legend", "prophecy", "rune", "elder",
        "primal", "eternal", "void", "abyss", "shadow", "nexus",
        "ascend", "ascension", "transcend", "threshold",
    ],

    # ── NATURALEZA ────────────────────────────────────────────────────────
    "NATURE": [
        "forest", "ocean", "river", "mountain", "volcano",
        "thunder", "lightning", "storm", "rain", "snow", "ice",
        "fire", "wind", "earth", "water", "tree", "flower",
        "seed", "leaf", "root", "jungle", "desert", "glacier",
        "avalanche", "landslide", "tornado", "hurricane", "tidal",
        "coral", "reef", "bloom", "sprout", "garden", "meadow",
        "vale", "cliff", "canyon", "island", "tide", "wave",
        "aurora", "rainbow", "mist", "fog", "dew", "petal",
    ],

    # ── NÚMEROS / ABSTRACTO ───────────────────────────────────────────────
    "NUMBERS": [
        "one", "two", "three", "four", "five", "six", "seven",
        "eight", "nine", "ten", "hundred", "thousand",
        "first", "second", "third", "number", "digit",
        "binary", "hexadecimal", "fibonacci", "prime",
    ],
}

def setup_db():
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS narrative_stats (
                id SERIAL PRIMARY KEY,
                narrative TEXT NOT NULL,
                token_count INTEGER DEFAULT 0,
                avg_volume NUMERIC(20,2) DEFAULT 0,
                total_volume NUMERIC(20,2) DEFAULT 0,
                avg_score NUMERIC(5,2) DEFAULT 0,
                top_token TEXT,
                top_token_volume NUMERIC(20,2) DEFAULT 0,
                momentum NUMERIC(10,2) DEFAULT 0,
                window_hours INTEGER DEFAULT 24,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_narrative_stats_time
                ON narrative_stats(calculated_at DESC);
        """)
        conn.commit()
        try:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("""
                ALTER TABLE discovered_tokens
                ADD COLUMN IF NOT EXISTS narrative TEXT DEFAULT NULL
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log.warning(f"ALTER TABLE skipped (lock timeout): {e}")
        cur.close()
        log.info("✅ Tablas de narrativas creadas")
    finally:
        pool.putconn(conn)

def classify_token_narrative(name, symbol, description):
    """Clasifica un token en una narrativa basada en su nombre/descripción"""
    text = " ".join(filter(None, [name, symbol, description])).lower()
    
    scores = {}
    for narrative, keywords in NARRATIVE_PATTERNS.items():
        score = sum(1 for kw in keywords if kw in text)
        if score > 0:
            scores[narrative] = score
    
    if not scores:
        return "OTHER"
    
    return max(scores, key=scores.get)

def tag_tokens_with_narratives():
    """Clasifica tokens sin narrativa Y reclasifica los que quedaron en OTHER"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        # NULL: tokens nuevos sin clasificar
        # OTHER reciente: pueden mejorar con los nuevos patrones
        cur.execute("""
            SELECT mint, name, symbol, description
            FROM discovered_tokens
            WHERE (narrative IS NULL OR narrative = 'OTHER')
            AND created_at > NOW() - INTERVAL '7 days'
            LIMIT 5000
        """)
        tokens = cur.fetchall()

        updates = []
        for mint, name, symbol, desc in tokens:
            narrative = classify_token_narrative(name, symbol, desc)
            updates.append((narrative, mint))

        if updates:
            cur.executemany(
                "UPDATE discovered_tokens SET narrative = %s WHERE mint = %s",
                updates
            )
            conn.commit()
            log.info(f"✅ {len(updates)} tokens clasificados/reclasificados")

        cur.close()
    finally:
        pool.putconn(conn)


def reclassify_all_other(batch_size=10000):
    """
    Reclasifica TODO el histórico de tokens con narrative = 'OTHER'.
    Ejecutar una vez tras actualizar NARRATIVE_PATTERNS.
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM discovered_tokens WHERE narrative = 'OTHER'")
        total = cur.fetchone()[0]
        log.info(f"🔄 Reclasificando {total:,} tokens en OTHER...")

        offset = 0
        total_improved = 0
        while True:
            cur.execute("""
                SELECT mint, name, symbol, description
                FROM discovered_tokens
                WHERE narrative = 'OTHER'
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (batch_size, offset))
            rows = cur.fetchall()
            if not rows:
                break

            updates = []
            for mint, name, symbol, desc in rows:
                narrative = classify_token_narrative(name, symbol, desc)
                if narrative != 'OTHER':
                    updates.append((narrative, mint))
                    total_improved += 1

            if updates:
                cur.executemany(
                    "UPDATE discovered_tokens SET narrative = %s WHERE mint = %s",
                    updates
                )
                conn.commit()

            offset += batch_size
            log.info(f"  Procesados {min(offset, total):,}/{total:,} | Mejorados: {total_improved:,}")

        log.info(f"✅ Reclasificación completa — {total_improved:,} tokens rescatados de OTHER")
        return total_improved
    finally:
        pool.putconn(conn)

def calculate_narrative_stats(window_hours=24):
    """Calcula estadísticas por narrativa para una ventana de tiempo"""
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                narrative,
                COUNT(*) as token_count,
                ROUND(AVG(volume_24h)::numeric, 2) as avg_volume,
                ROUND(SUM(volume_24h)::numeric, 2) as total_volume,
                ROUND(AVG(survival_score)::numeric, 2) as avg_score,
                MAX(name) as top_name,
                MAX(volume_24h) as top_volume
            FROM discovered_tokens
            WHERE created_at > NOW() - INTERVAL '%s hours'
            AND volume_24h > 0
            AND narrative IS NOT NULL
            GROUP BY narrative
            ORDER BY total_volume DESC
        """ % window_hours)
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        pool.putconn(conn)

def calculate_momentum(narrative, window_hours=6):
    """
    Momentum = crecimiento de volumen en últimas 6h vs 6h anteriores
    """
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COALESCE(SUM(CASE WHEN created_at > NOW() - INTERVAL '%s hours' 
                    THEN volume_24h END), 0) as recent_vol,
                COALESCE(SUM(CASE WHEN created_at BETWEEN NOW() - INTERVAL '%s hours' 
                    AND NOW() - INTERVAL '%s hours' 
                    THEN volume_24h END), 0) as prev_vol
            FROM discovered_tokens
            WHERE narrative = %%s
            AND volume_24h > 0
        """ % (window_hours, window_hours*2, window_hours), (narrative,))
        row = cur.fetchone()
        cur.close()
        
        if not row or not row[1] or row[1] == 0:
            return 0
        
        recent, prev = float(row[0]), float(row[1])
        if prev == 0:
            return 100 if recent > 0 else 0
        
        return round((recent - prev) / prev * 100, 1)
    finally:
        pool.putconn(conn)

def save_narrative_stats(stats, window_hours):
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        for narrative, count, avg_vol, total_vol, avg_score, top_name, top_vol in stats:
            momentum = calculate_momentum(narrative)
            cur.execute("""
                INSERT INTO narrative_stats 
                (narrative, token_count, avg_volume, total_volume, avg_score, 
                 top_token, top_token_volume, momentum, window_hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (narrative, count, avg_vol, total_vol, avg_score,
                  top_name, top_vol, momentum, window_hours))
        conn.commit()
        cur.close()
    finally:
        pool.putconn(conn)

def print_narrative_report(stats, window_hours):
    print("\n" + "="*70)
    print(f"📡 NARRATIVE INTELLIGENCE — Últimas {window_hours}h — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*70)
    
    for narrative, count, avg_vol, total_vol, avg_score, top_name, top_vol in stats[:15]:
        momentum = calculate_momentum(narrative)
        
        if momentum > 50:
            trend = "🚀 PUMPING"
        elif momentum > 20:
            trend = "📈 RISING"
        elif momentum > 0:
            trend = "➡️  STABLE"
        elif momentum < -20:
            trend = "📉 FADING"
        else:
            trend = "⬇️  DYING"
        
        print(f"\n  {trend} | {narrative:<15}")
        print(f"    Tokens:      {count}")
        print(f"    Vol Total:   ${total_vol:>12,.0f}")
        print(f"    Vol Avg:     ${avg_vol:>12,.0f}")
        print(f"    Momentum:    {momentum:+.1f}%")
        print(f"    Score Avg:   {avg_score or 0:.1f}")
        print(f"    Top Token:   {top_name or 'Unknown'}")
    
    print("\n" + "="*70)
    
    # Top 3 narrativas emergentes
    print("\n🔥 TOP 3 NARRATIVAS EMERGENTES AHORA:")
    emerging = sorted(
        [(n, c, av, tv, sc, tn, tv2) for n, c, av, tv, sc, tn, tv2 in stats if c >= 2],
        key=lambda x: calculate_momentum(x[0]),
        reverse=True
    )[:3]
    
    for i, (narrative, count, avg_vol, total_vol, avg_score, top_name, top_vol) in enumerate(emerging, 1):
        momentum = calculate_momentum(narrative)
        print(f"  #{i} {narrative} — {count} tokens — Vol: ${total_vol:,.0f} — Momentum: {momentum:+.1f}%")
    
    print("="*70 + "\n")

def run():
    setup_db()
    log.info("📡 Narrative Engine iniciando...")
    
    log.info("🏷️  Clasificando tokens...")
    tag_tokens_with_narratives()
    
    for window in [6, 24]:
        log.info(f"📊 Calculando stats para ventana {window}h...")
        stats = calculate_narrative_stats(window)
        save_narrative_stats(stats, window)
        
        if window == 24:
            print_narrative_report(stats, window)
    
    log.info("✅ Narrative Engine completado")

if __name__ == "__main__":
    run()
