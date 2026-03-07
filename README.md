# 🚀 Solana Token Intelligence Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Solana](https://img.shields.io/badge/Solana-Mainnet-purple)
![Status](https://img.shields.io/badge/Status-Production-brightgreen)

Real-time data intelligence platform that captures, processes and analyzes every token launched on Solana's Pump.fun — detecting emerging narratives before the market does.

---

## 🏗️ Architecture
cat > ~/solana_bot/README.md << 'EOF'
# 🚀 Solana Token Intelligence Platform

![Python](https://img.shields.io/badge/Python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)
![Solana](https://img.shields.io/badge/Solana-Mainnet-purple)
![Status](https://img.shields.io/badge/Status-Production-brightgreen)

Real-time data intelligence platform that captures, processes and analyzes every token launched on Solana's Pump.fun — detecting emerging narratives before the market does.

---

## 🏗️ Architecture
```
Solana Blockchain (Pump.fun)
         │
         ▼
┌─────────────────────┐
│   pump_harvester    │  WebSocket listener — captures new tokens in < 1 second
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│    PostgreSQL DB    │  4 tables — tokens, trends, keywords, alerts
└────────┬────────────┘
         │
    ┌────┴─────┬──────────────┐
    ▼          ▼              ▼
metadata_  price_fetcher  trend_engine
fetcher    (DexScreener)  (NLP + TF-IDF)
    │          │              │
    └────┬─────┘              │
         ▼                    ▼
┌─────────────────────────────────┐
│         FastAPI REST API        │
│   /tokens  /trends  /stats      │
│   /trends/emerging  /search     │
└─────────────────────────────────┘
```

---

## ⚡ Key Features

- **Real-time capture** — Detects every new Pump.fun token in under 1 second via WebSocket
- **Price intelligence** — Fetches initial price 10 seconds after launch via DexScreener
- **Trend detection** — NLP engine analyzes token names to detect emerging narratives
- **REST API** — Full FastAPI with auto-generated Swagger documentation
- **Production-grade** — All services run as systemd daemons with auto-restart
- **Structured logging** — Professional logging with rotation in `/var/log/solana_bot/`

---

## 📊 Stats (Live)

| Metric | Value |
|--------|-------|
| Tokens captured | 68,000+ |
| Capture rate | ~900 tokens/hour |
| Uptime | 24/7 |
| API endpoints | 7 |
| DB tables | 4 |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Blockchain listener | Python + WebSockets |
| Database | PostgreSQL 16 |
| API Framework | FastAPI + Uvicorn |
| Price data | DexScreener API |
| Metadata | Helius RPC |
| NLP | Regex + TF-IDF |
| Process management | systemd |
| Async processing | asyncio + aiohttp |

---

## 🚀 Services Running in Production
```bash
systemctl status pump-harvester      # Token capture 24/7
systemctl status metadata-fetcher    # Metadata enrichment every 5 min
systemctl status price-fetcher       # Price updates every 30 sec
systemctl status trend-engine        # Trend analysis every hour
systemctl status solana-api          # REST API on port 8000
```

---

## 📡 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Platform info |
| GET | `/health` | System health check |
| GET | `/tokens` | Recent tokens with filters |
| GET | `/tokens/search?q=dog` | Search tokens by name |
| GET | `/trends` | Top keywords by time window |
| GET | `/trends/emerging` | Emerging narratives detection |
| GET | `/stats` | Platform statistics |

---

## 🗄️ Database Schema
```sql
discovered_tokens   -- Core token data + price + metadata
keyword_index       -- NLP keyword index per token
trend_analysis      -- Hourly trend aggregations
alert_log           -- System alerts history
```

---

## ⚙️ Setup
```bash
# Clone the repo
git clone https://github.com/Douglas94vzla/solana-token-intelligence.git
cd solana-token-intelligence

# Create virtual environment
python3 -m install venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials

# Setup database
psql -U postgres -f schema.sql

# Start services
systemctl start pump-harvester
systemctl start metadata-fetcher.timer
systemctl start price-fetcher
systemctl start trend-engine.timer
systemctl start solana-api
```

---

## 📁 Project Structure
```
solana_bot/
├── pump_harvester.py      # WebSocket blockchain listener
├── metadata_fetcher.py    # Helius RPC metadata enrichment
├── price_fetcher.py       # DexScreener price intelligence
├── trend_engine.py        # NLP trend detection engine
├── api.py                 # FastAPI REST API
├── requirements.txt       # Python dependencies
├── .env.example           # Environment template
└── README.md              # This file
```

---

## 👨‍💻 Author

**Douglas Alvarez** — [@Douglas94vzla](https://github.com/Douglas94vzla)

> Built as part of a professional data engineering portfolio demonstrating real-time blockchain data pipelines, NLP trend analysis, and production-grade API development.

