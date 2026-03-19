# CHANGELOG — Solana Token Intelligence Bot

Registro diario de cambios, mejoras y bugs resueltos.
Formato: problema → solución → resultado.

---

## 2026-03-19

### Bugs críticos resueltos
- **Sin trades en 5 días** — entry_signal solo buscaba tokens de los últimos 90 min. Ventana ampliada a 3h. Resultado: watchlist subió de 6 a 19 tokens.
- **Stop loss sangrando (-55% promedio en vez de -30%)** — cuando DexScreener no devuelve precio, el trade quedaba abierto indefinidamente hasta colapsar. Fix: si no hay precio por 60s (6 ciclos de 10s), cierra automáticamente al stop máximo (-30%).
- **Duplicados en wallet_watcher** — mismo evento logueado 2 veces por carrera en `ON CONFLICT DO NOTHING`. Fix: solo loguear si `rowcount > 0`.

### Mejoras ML
- Modelo reentrenado con 770 muestras (antes 286) → AUC 0.673 → **0.712**
- Calibración isotónica aplicada: probabilidades ahora reflejan tasas reales (~23% base rate)
- Regularización más fuerte: RF max_depth=6, XGBoost reg_alpha=0.1 / lambda=1.5
- Threshold bajado 65% → **35%** para coincidir con rango de probabilidades calibradas
- Estrategias: CONSERVATIVE≥50%, STANDARD/AGGRESSIVE≥35%

### Bonding curve pricing (nuevo)
- El 96% de tokens pump.fun viven en bonding curve antes de graduarse a Raydium — DexScreener no los ve.
- Nuevo módulo en `price_fetcher.py`: deriva PDA del bonding curve, llama `getMultipleAccounts` en batches de 100, calcula precio desde reserves SOL/token.
- Estima `buys_5m` desde SOL real en el pool (avg 0.1 SOL/buy).
- Resultado: cobertura de precio 4% → **72%** de tokens nuevos.

### Filtros de calidad ajustados
- Filtro de liquidez ($10k) ya no aplica a tokens en bonding curve (no tienen `pair_address`).
- Mínimo de liquidez para tokens con par DexScreener: $10k → $3k.
- Filtro de "sin redes sociales" eliminado (el 99% de tokens no tiene socials aún, el backfill lo resolverá).
- `market_cap` mínimo en `check_new_signals`: $10k → $2k.

### Backfill socials (nuevo timer)
- 95k tokens con nombre pero sin twitter/telegram/website.
- `backfill_socials.py` corre cada 5min via systemd timer, 200 tokens/batch.
- Resultado primer ciclo: 24/200 tokens con socials encontrados (~12% hit rate).
- Tiempo estimado para backfill completo: ~1.5-2 días.

### Estado al cierre del día
- Capital paper: $1,117 (+11.7%) | 27 trades cerrados | Win rate: 44%
- Watchlist: 19 tokens | Cobertura de precio: 72% | Modelo: RandomForest AUC=0.740

---

## 2026-03-18

### Metadata via Token-2022 (reescritura)
- Descubierto que pump.fun usa el programa **TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb** (Token-2022), NO Metaplex.
- `metadata_fetcher.py` reescrito: lee extensión `tokenMetadata` del account info → obtiene URI → fetch a IPFS/Arweave → extrae description, image, twitter, telegram, website.
- Cobertura alcanzada: twitter=331, telegram=5, website=184, description=430.

### Fixes de datos
- `price_fetcher.py` ahora captura `priceChange.m5/h1/h6/h24` de DexScreener → llena columnas `price_change_*` (antes siempre 0).
- `liquidity_usd` ahora se preserva con `COALESCE` en updates.

### Narrativas expandidas
- NARRATIVE_PATTERNS ampliado de 17 → 25 categorías.
- Categoría OTHER bajó de 83.5% → 73.3%.

---

## 2026-03-17

### Fix data leakage en ML
- El modelo antiguo entrenaba con features del estado ACTUAL del token (incluyendo precio futuro) → inflaba artificialmente el AUC.
- Fix: tabla `token_features_at_signal` captura una foto exacta de todas las features en el instante del primer BUY/STRONG_BUY, antes de que el precio se mueva.
- Primer entrenamiento limpio: GradientBoosting AUC=0.877 con 2,894 muestras.

---

## 2026-03-16

### Historial del deployer
- Nueva tabla `deployer_stats` con: tokens lanzados, cuántos rugaron, rug rate, flag `is_serial_rugger`.
- Nuevas features en ML: `deployer_prior_tokens`, `deployer_rugged_count`, `deployer_rug_rate`, `is_known_rugger`.
- Si un deployer tiene >2 rugs y rug_rate >50% → bloqueado en entry_signal.

---

## 2026-03-15

### Paper trading — mejoras masivas (14 en total)
- **Kelly sizing**: posición adaptativa según probabilidad ML (1-5%, half-Kelly).
- **Stop adaptativo ATR**: stop loss entre -15% y -30% según volatilidad del token.
- **Trailing stop**: cierra si cae 20% desde el pico (activa tras +10% de ganancia).
- **Multi-estrategia**: CONSERVATIVE (ML≥80, 1%), STANDARD (ML≥65, Kelly), AGGRESSIVE (ML≥65, 5%) corren en paralelo con capital virtual separado.
- **Slippage dinámico**: calcula impacto real según trade_size/liquidity (antes fijo 3%).
- **Pump burst detector**: 4+ snapshots consecutivos subiendo → bonus de momentum +30pts.
- **Rug re-check**: tokens con ENTER señal se re-analizan cada 30min para detectar rugs tardíos.
- **Jupiter fallback**: si DexScreener falla, usa Jupiter Price API para precio.
- **Cooldown post-trailing**: 15min antes de re-entrar en el mismo token tras trailing stop.
- **Ciclo dividido**: posiciones revisadas cada 10s (antes 60s), señales nuevas cada 60s.
- **Fix crítico**: `manage_open_trades()` ahora corre SIEMPRE, incluso si el límite diario está activo (antes se saltaba la gestión de posiciones abiertas).
- **Backtester integrado**: corre automáticamente después de cada reentrenamiento ML.

### Dashboard fix
- `dashboard.py` causaba deadlocks en PostgreSQL al no cerrar conexiones.
- Fix: `finally: conn.close()` en función `query()`.
- PostgreSQL configurado: `idle_in_transaction_session_timeout = 30s`.

### Velocity features en ML
- Nuevas features: `buys_growth`, `vol_growth_1h`, `buy_acceleration` → aceleración vs promedio de 24h.
- Smart money y narrative momentum integrados como features ML.

---

## 2026-03-13

### Filtros anti-rug en entry_signal
- `ONLY_0_HOLDERS`: bloquea tokens sin holders on-chain.
- Liquidez mínima: tokens con par DexScreener pero sin liquidez → bloqueados.
- Endpoint WebSocket con rotación automática (Helius → Ankr → Oficial) con backoff.

---

## 2026-03-12

### Fix: tokens pre-DexScreener
- `check_new_signals()` bloqueaba tokens con `liquidity_usd IS NULL`.
- Fix: tokens sin liquidez pasan si `market_cap >= 20,000`.

---

## 2026-03-11

### Bugs críticos de producción resueltos
- **Deadlock PostgreSQL 10h+**: `dashboard.py` dejaba conexiones `idle in transaction` → bloqueaba `ALTER TABLE discovered_tokens`. Fix: cerrar conexiones + `lock_timeout='5s'` en todos los `setup_db()`.
- **Crash loop survival_scorer**: `Type=simple + Restart=always` en servicio oneshot → 7,155 reinicios. Fix: `Type=oneshot`.
- **Logs duplicados**: scripts con `FileHandler` + servicio con `StandardOutput` al mismo archivo. Fix: remover `StandardOutput=` de los services.
- **wallet_watcher roto**: usaba WebSocket por wallet → HTTP 429 inmediato. Reescrito a HTTP polling cada 60s.

### Servicios nuevos activados
- `rug-detector.timer` — cada 10min
- `narrative-engine.timer` — cada 1h
- `ml-trainer.timer` — todos los días 03:00

---

## 2026-03-09 → 2026-03-10

### Construcción inicial del pipeline completo

**Capa 1 — Captura** (`pump_harvester.py`):
- WebSocket a Solana para logsSubscribe del programa pump.fun.
- Decodifica base64 para extraer mint addresses.
- Fetch inicial de precio DexScreener 10s después del descubrimiento.

**Capa 2 — Scoring** (`survival_scorer.py`):
- Score 0-100 basado en buys_5m, sells_5m, market_cap, volumen, metadata.
- Señales: STRONG_BUY / BUY / WATCH / IGNORE.

**Capa 3 — Señal de entrada** (`entry_signal.py`):
- Momentum score desde historial de price_snapshots.
- Filtros: rug_score + ML probability.
- Señales: ENTER / WATCH / EXIT / WAIT.

**Capa 4 — Paper trading** (`paper_trading.py`):
- Abre trades en señales ENTER con ML ≥ 65%.
- Take profit +50%, stop loss -30%, timeout 2h.
- Alertas Telegram.

**ML** (`ml_model.py`):
- RandomForest + GradientBoosting + XGBoost.
- Selección por AUC-ROC en 5-fold cross-validation.
- Target: ¿sube +30% en las 2h siguientes al signal?

**Soporte**:
- `api.py` — FastAPI REST en puerto 8000.
- `dashboard.py` — Streamlit en puerto 8501.
- `rug_detector.py` — holders, mint authority, sell pressure.
- `narrative_engine.py` — clasificación por regex en 25 categorías.
- `wallet_watcher.py` — tracking de smart wallets.
- `telegram_bot.py` — alertas de trades y resumen diario.

---

## Arquitectura del pipeline (resumen)

```
pump.fun WebSocket
       ↓
pump_harvester.py      → discovered_tokens (mint)
       ↓
metadata_fetcher.py    → name, symbol, uri → socials (Token-2022)
price_fetcher.py       → price_usd, market_cap (DexScreener + bonding curve)
       ↓
survival_scorer.py     → survival_score, signal (BUY/STRONG_BUY/WATCH/IGNORE)
rug_detector.py        → rug_score, rug_flags
narrative_engine.py    → narrative (AI/MEME/POLITICS/etc)
       ↓
entry_signal.py        → momentum + ML filter → entry_signal (ENTER/EXIT)
       ↓
paper_trading.py       → abre/cierra trades, gestiona capital
       ↓
telegram_bot.py        → alertas
```

## Métricas actuales (Mar 19)
- Tokens descubiertos: ~260k total | ~18k/día
- Capital paper: $1,117 | PnL: +$117 (+11.7%)
- Trades: 27 cerrados | Win rate: 44% | Avg PnL: +22%
- Modelo ML: RandomForest AUC=0.740 | 770 muestras
- Cobertura de precio: 72% de tokens nuevos
- Socials: 2,423 tokens con twitter (backfill en curso)
