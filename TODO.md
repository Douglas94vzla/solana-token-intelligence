# TODO — Solana Bot Mejoras Pendientes

> Revisión programada: **29 de abril de 2026** (fin del mes de observación)
> NO implementar antes de esa fecha salvo bugs críticos.

---

## BUGS ACTIVOS

### BUG BLOQUEANTE: token_features_at_signal — features críticas siempre en 0
> **Verificado 08/04/2026 — NO reactivar ml-trainer.timer hasta resolver esto**

**Síntoma:** 71,403 muestras acumuladas pero completamente inutilizables para reentrenar:
- `liquidity_usd > 0`: 0.0% de las muestras (feature más importante)
- `narrative != OTHER`: 0.0% de las muestras
- `volume_1h > 0`: 0.7% | `fdv > 0`: 3.7%

**Causa raíz:** `survival_scorer.py::capture_features_at_signal()` captura la foto del token
en el momento del primer BUY/STRONG_BUY — milisegundos después del lanzamiento.
En ese momento: `price_fetcher` no ha corrido aún (no hay pair en DexScreener),
`narrative_engine` no ha corrido aún (corre cada hora).
Resultado: 71k muestras con liquidity=0 y narrative=OTHER para el 100%.

**Consecuencia:** Reentrenar hoy con estos datos crearía un train-inference mismatch
(el modelo aprendería sin liquidez/narrative, pero entry_signal.py predice CON esos datos).
El modelo actual (Mar 19, AUC=0.766, 782 samples) sigue siendo mejor.

**Fix requerido antes de reactivar el timer:**
- Opción A: `capture_features_at_signal()` se mueve a `entry_signal.py` — capturar
  la foto en el momento del ENTER (cuando el token ya tiene par en DexScreener y narrative)
- Opción B: Añadir un UPDATE en `survival_scorer.py` que refresque
  `liquidity_usd`, `narrative`, `fdv`, `volume_1h` en registros existentes
  de `token_features_at_signal` cuando esos campos estén disponibles (lazy update)
- **Opción recomendada: A** — más limpia, captura justo cuando tenemos todos los datos

**Archivos a tocar:** `entry_signal.py` (mover captura al ENTER), `survival_scorer.py`
(eliminar o simplificar `capture_features_at_signal`), `ml_model.py` (verificar clean_count)

---

### ml-trainer.timer — DESACTIVADO (bloqueado por bug arriba)
- `Active: inactive (dead)` — el modelo NO se está reentrenando
- **NO activar hasta resolver el bug de token_features_at_signal**
- Una vez resuelto y acumuladas ~2,000 muestras limpias: `systemctl enable --now ml-trainer.timer`
- Impacto actual: modelo de Mar 19 sigue en producción (AUC=0.766, 782 samples)

---

## A. ENSEMBLE ML — "Varios modelos en uno"
> Inspiración: Renaissance Technologies, Two Sigma

**Descripción:** En vez de entrenar RF + GradientBoosting + XGBoost y descartar los 2 perdedores,
usarlos todos juntos con **weighted soft voting**: cada modelo aporta su probabilidad,
se promedian ponderados por su AUC-ROC del cross-validation.

```
prob_final = (prob_RF × auc_RF + prob_GB × auc_GB + prob_XGB × auc_XGB)
             / (auc_RF + auc_GB + auc_XGB)
```

**Archivos a tocar:**
- `ml_model.py`: `train_models()` entrena los 3, `save_model()` guarda los 3 con sus pesos en `model.pkl`
- `entry_signal.py`: `load_ml_model()` + `ml_predict()` detectan el nuevo formato y promedian

**Variante hard voting (más conservadora):**
- Solo ENTER si 2/3 modelos superan el threshold → reduce falsos positivos

**Beneficio esperado:** +2-4% AUC-ROC, menos varianza en predicciones

---

## B. REGIME DETECTION — Detectar el estado del mercado
> Inspiración: Two Sigma, Bridgewater

**Descripción:** Antes de abrir trades, evaluar si el mercado está en modo
"risk-on" (muchos pumps, buen momento) o "risk-off" (muchos rugs, mercado bajista).
Ajustar thresholds automáticamente según el régimen.

**Métricas para el régimen:**
- WR de los últimos 20 trades (ventana deslizante)
- % de tokens con rug_score > 60 en las últimas 4h
- Ratio ENTER/total señales en las últimas 2h
- Precio de SOL/BTC (si baja >5% en 1h → risk-off automático)

**Acciones por régimen:**
- `RISK_ON`: thresholds normales (ML ≥ 65%, liquidez ≥ $5k)
- `RISK_OFF`: ML ≥ 75%, liquidez ≥ $8k, MAX_OPEN = 1

**Archivos a tocar:** `entry_signal.py` (nueva función `get_market_regime()`),
`paper_trading.py` (usa el régimen para decidir si abrir)

---

## C. VOLATILITY TARGETING — Tamaño dinámico por volatilidad
> Inspiración: AQR Capital, Winton Group

**Descripción:** Reducir el tamaño de la posición cuando el mercado está volátil.
Medir la volatilidad del mercado como la desviación estándar del % de cambio de precio
de los últimos N tokens cerrados.

**Fórmula:**
```
vol_actual = std(pnl_pct de últimas 20 posiciones cerradas)
target_vol = 0.25  # 25% de volatilidad objetivo
size_factor = min(1.0, target_vol / vol_actual)
trade_size = base_size × size_factor
```

**Archivos a tocar:** `paper_trading.py` (nueva función `get_vol_adjusted_size()`)

---

## D. DRAWDOWN-BASED SIZING — Reducir tamaño en racha perdedora
> Inspiración: Man AHL, Millennium

**Descripción:** Si el capital cae más de X% desde el pico histórico,
reducir automáticamente el tamaño de las posiciones.

**Reglas:**
- Drawdown < 5%: size normal (Kelly)
- Drawdown 5-10%: size × 0.75
- Drawdown > 10%: size × 0.50 + parar AGGRESSIVE
- Drawdown > 15%: pausar todas las estrategias, alertar por Telegram

**Archivos a tocar:** `paper_trading.py` (nueva función `get_drawdown_factor()`)

---

## E. CONCENTRATION RISK — No acumular el mismo riesgo
> Inspiración: Citadel, gestión de riesgo estándar

### E1. Mismo funding wallet / deployer
- Si ya hay un trade abierto de un deployer/funding_wallet → NO abrir otro del mismo
- Ya tenemos `funding_wallet` en `discovered_tokens` desde Mar 23

### E2. Mismo narrative
- No más de 2 trades abiertos del mismo narrative simultáneamente
- Ej: si ya tenemos 2 tokens AI/AGI abiertos, esperar a que cierre uno

**Archivos a tocar:** `paper_trading.py` en `check_new_signals()`

---

## F. CAPITAL ALLOCATION DINÁMICA — El mejor strategy gana más capital
> Inspiración: multi-manager hedge funds

**Descripción:** Reasignar capital semanalmente al strategy con mejor Sharpe ratio
de los últimos 7 días. El strategy con peor performance reduce su capital.

**Regla simple:**
```
sharpe_7d = mean(daily_pnl_pct) / std(daily_pnl_pct) × sqrt(7)
capital_nuevo = capital_total × softmax(sharpe_scores)
```

**Archivos a tocar:** `paper_trading.py` (nueva función `rebalance_strategy_capital()`),
ejecutar semanalmente (nuevo timer o dentro del daily summary)

---

## G. CROSS-ASSET FILTER — Pausar si SOL/BTC cae
> Inspiración: correlación macro estándar en quant trading

**Descripción:** Si SOL cae más de 5% en 1h o BTC cae más de 3% en 1h,
pausar apertura de nuevas posiciones temporalmente (30-60 min).

**Fuente de datos:** CoinGecko API (gratis) o DexScreener SOL/USDC pair

**Archivos a tocar:** `entry_signal.py` o `paper_trading.py`

---

## H. ADAPTIVE TAKE PROFIT — Extender si el momentum sigue
> Inspiración: trend-following (AHL, Winton)

**Descripción:** En vez de cerrar siempre a +50% fijo, si el momentum
sigue fuerte (buys_5m sigue acelerando, buy_sell_ratio > 3), extender
el trailing en 10% adicionales antes de cerrar el resto.

**Condición para extender:**
- Precio actual > TAKE_PROFIT
- buys_5m actuales > entry buys_5m × 1.5
- peak_price sigue subiendo (último snapshot > anterior)

**Archivos a tocar:** `paper_trading.py` en `manage_open_trades()`

---

## I. WALK-FORWARD VALIDATION — Evitar data leakage en ML
> Estándar en quant finance

**Descripción:** En vez de CV aleatorio, usar bloques temporales:
entrenar en semanas 1-3, validar en semana 4, nunca mezclar futuro con pasado.

**Implementación:** `TimeSeriesSplit` de sklearn en vez de `StratifiedKFold`

**Archivos a tocar:** `ml_model.py` en `train_models()`

---

## J. FEATURE DRIFT DETECTION — Alertar si el mercado cambió
> Inspiración: MLOps best practices, Two Sigma

**Descripción:** Comparar la distribución de features del modelo entrenado
vs las features de los últimos tokens. Si hay drift significativo
(KS test p < 0.05 en más de 5 features), alertar por Telegram.

**Archivos a tocar:** `ml_model.py` (añadir al final de `run()`),
`telegram_bot.py` (nueva función `alert_feature_drift()`)

---

## PRIORIDAD SUGERIDA (para el 29 de abril)

| # | Estrategia | Impacto esperado | Complejidad |
|---|---|---|---|
| 1 | **Ensemble ML** | Alto — mejor AUC, menos varianza | Media |
| 2 | **Regime Detection** | Alto — evita operar en mercado malo | Media |
| 3 | **Concentration Risk** | Medio — reduce rugs correlacionados | Baja |
| 4 | **Drawdown Sizing** | Medio — protege capital en rachas malas | Baja |
| 5 | **Walk-forward CV** | Medio — ML más honesto | Baja |
| 6 | **Volatility Targeting** | Medio — sizing más inteligente | Media |
| 7 | **Capital Allocation** | Medio — maximiza estrategia ganadora | Media |
| 8 | **Cross-asset Filter** | Variable — depende de correlación SOL | Baja |
| 9 | **Adaptive TP** | Bajo-Medio — captura más upside | Alta |
| 10 | **Feature Drift** | Bajo — operacional/alertas | Baja |

---

*Última actualización: 2026-04-08*
*Basado en conversaciones del 7-8 de abril de 2026*
