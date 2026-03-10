# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies (venv already contains them)
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env — required vars: WSS_URL, RPC_URL, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST
# Optional (for Telegram alerts): TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
```

## Running Services

All services are managed as systemd daemons in production:

```bash
systemctl status pump-harvester       # WebSocket token capture
systemctl status metadata-fetcher     # Helius RPC enrichment (timer)
systemctl status price-fetcher        # DexScreener price updates
systemctl status trend-engine         # NLP trend analysis (timer)
systemctl status solana-api           # FastAPI REST on port 8000
systemctl status solana-dashboard     # Streamlit dashboard on port 8501
```

Run individual scripts directly for development/testing:

```bash
python pump_harvester.py       # Starts WebSocket listener
python entry_signal.py         # Starts signal engine (60s cycle)
python paper_trading.py        # Starts paper trading engine (60s cycle)
python rug_detector.py         # One-shot rug analysis pass
python survival_scorer.py      # One-shot scoring pass
python ml_model.py             # Trains and saves ML model
python narrative_engine.py     # One-shot narrative classification
python backtester.py           # Backtests strategy on historical DB data
python api.py                  # Starts FastAPI server
python dashboard.py            # Starts Streamlit dashboard
```

Logs are written to `/var/log/solana_bot/` — each module has its own log file (e.g. `entry_signal.log`, `paper_trading.log`).

## Architecture Overview

This is a real-time Solana token intelligence and paper-trading platform targeting Pump.fun meme token launches.

### Data Pipeline (in order)

1. **`pump_harvester.py`** — Subscribes to Solana WebSocket (`WSS_URL`) for `logsSubscribe` events from the Pump.fun program (`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`). Decodes base64 program data to extract mint addresses. Saves new tokens to `discovered_tokens` and asynchronously fetches initial DexScreener price 10s after detection.

2. **`metadata_fetcher.py`** — Enriches tokens via Helius RPC (name, symbol, image, decimals). Runs periodically.

3. **`survival_scorer.py`** — Scores each token 0–100 based on buys_5m, sells_5m, market_cap, volume, and metadata presence. Assigns `signal` values: `STRONG_BUY`, `BUY`, `WATCH`, `IGNORE`.

4. **`rug_detector.py`** — For `STRONG_BUY`/`BUY` tokens: queries Solana RPC for holder concentration (top10%), mint/freeze authority status, and sell pressure from DexScreener. Assigns `rug_score` (0–100) and `rug_flags`.

5. **`narrative_engine.py`** — Classifies each token's name/symbol into predefined narrative categories (e.g. AI/AGI, MEME/DOG, POLITICS) via regex pattern matching. Saves `narrative` column.

6. **`entry_signal.py`** — The core signal engine. Every 60s: fetches live DexScreener data, saves `price_snapshots`, computes momentum score from price history, then applies two sequential filters: **rug filter** (blocks if rug_score ≥ 60) and **ML filter** (blocks if ML probability < 65%). Generates `ENTER`/`WATCH`/`EXIT`/`WAIT` signals.

7. **`paper_trading.py`** — Paper trading engine (60s cycle). Opens trades on `ENTER` signals with ml_probability ≥ 65%, simulates 3% slippage + 0.5% fees. Closes on: +50% take-profit, -30% stop-loss, or 2-hour timeout. Max 3 simultaneous trades. Sends Telegram alerts via `telegram_bot.py`.

### ML Layer

**`ml_model.py`** — Trains on historical `discovered_tokens` joined with `price_snapshots`. Target: did price reach +50% gain from entry? Trains RandomForest, GradientBoosting, and XGBoost; selects best by AUC-ROC via 5-fold cross-validation. Saves `model.pkl` and `features.pkl`.

Feature set (21 features): raw market data + engineered ratios (`buy_sell_ratio`, `net_buy_pressure`, `vol_to_mcap`, `log_mcap`, `log_volume`) + risk flags (`holder_risk`, `zero_sells`, `optimal_mcap`).

The model is loaded at startup in `entry_signal.py`. Falls back to manual rules if model files are absent. Minimum 50 tokens with ≥10 snapshots required to train.

### Database Schema (PostgreSQL)

```
discovered_tokens     — Core token data (mint as PK), survival score, signals, ML probability
price_snapshots       — Time-series price/volume/buys/sells per mint
paper_trades          — Paper trading history (OPEN/CLOSED)
paper_capital         — Single-row capital tracker (wins, losses, PnL)
keyword_index         — NLP keyword index per token
trend_analysis        — Hourly trend aggregations
alert_log             — System alert history
```

### Supporting Modules

- **`api.py`** — FastAPI REST: `/tokens`, `/trends`, `/trends/emerging`, `/stats`, `/search`
- **`dashboard.py`** — Streamlit real-time dashboard
- **`telegram_bot.py`** — Notification helpers (`alert_enter`, `alert_paper_trade`, `alert_daily_summary`, `alert_system_status`). Requires `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID` env vars — silently no-ops if missing.
- **`price_fetcher.py`** — Periodic DexScreener price refresh for active tokens
- **`trend_engine.py`** — TF-IDF based trend analysis and emerging narrative detection
- **`wallet_watcher.py`** / **`smart_money.py`** — Track known smart wallet addresses
- **`backtester.py`** — Backtests filter strategy on historical DB data

### Key Constants (in source)

| File | Constant | Default |
|------|----------|---------|
| `entry_signal.py` | `ML_THRESHOLD` | 0.65 (65%) |
| `entry_signal.py` | `RUG_THRESHOLD` | 60 |
| `paper_trading.py` | `TAKE_PROFIT` | 1.50 (+50%) |
| `paper_trading.py` | `STOP_LOSS` | 0.70 (-30%) |
| `paper_trading.py` | `MAX_OPEN_TRADES` | 3 |
| `paper_trading.py` | `TRADE_SIZE_PCT` | 0.02 (2%) |
| `ml_model.py` | `TARGET_GAIN` | 0.5 (50%) |
| `ml_model.py` | `MIN_SNAPSHOTS` | 10 |

### Hardcoded Paths

- `.env` file: `/root/solana_bot/.env` (hardcoded in most modules via `load_dotenv('/root/solana_bot/.env')`)
- Model files: `/root/solana_bot/model.pkl` and `/root/solana_bot/features.pkl`
- Log directory: `/var/log/solana_bot/`
