# Roadmap → Producción Real — 1 Junio 2026
Capital objetivo: $150

## Estado actual (02/05/2026)
- Sistema paper trading: ✅ funcional
- Tests: 32/32 pasando
- Código real trading (wallet/swap): ❌ no existe
- Bugs críticos resueltos: 5 (NEAR_ZERO_LIQUIDITY, doble entrada, SQL injection, logrotate, índices DB)

---

## Semana 1 — 02/05 → 08/05: Fundación sólida ✅ EN PROGRESO
- [x] Framework de tests (pytest) — 32 tests
- [x] Bug NEAR_ZERO_LIQUIDITY corregido
- [x] Bug doble entrada corregido
- [x] SQL injection api.py corregido
- [x] 7 índices DB creados
- [x] Logrotate entry_signal (50MB max)
- [x] DB limpia (987k snapshots eliminados)
- [x] Telegram: solo compra/venta/logro/resumen 6PM
- [x] Sábado modo cauteloso (ML>=90%, 50% sizing, 2 ventanas)
- [x] Alerta: bot idle >4h en horario activo
- [ ] Idempotency constraint en paper_trades (unique mint+strategy+date)
- [ ] Retry + backoff para DexScreener/Jupiter
- [ ] Graceful shutdown (SIGTERM handler)

## Semana 2 — 09/05 → 15/05: Confiabilidad
- [ ] Circuit breaker para APIs externas
- [ ] Reentrenamiento automático del modelo (semanal via cron)
- [ ] price_snapshots: retención automática (borrar >30d diariamente)
- [ ] Aumentar tests: 32 → 60 (cobertura de close_trade, manage_open_trades)
- [ ] Revisión domingo (actualmente OFF — analizar si activar con ML>=95%)
- [ ] statement_timeout en conexiones DB (30s max)

## Semana 3 — 16/05 → 22/05: Ejecución Real
- [ ] Módulo wallet.py: cargar keypair Solana desde .env (PRIVATE_KEY)
- [ ] Módulo executor.py: swap via Jupiter API v6
- [ ] Ejecución real con montos mínimos en DEVNET ($0 en riesgo)
- [ ] Manejo de fallos de transacción (retry, slippage real vs simulado)
- [ ] Umbral mínimo de trade ($5 real — por debajo no vale la pena vs gas)
- [ ] CONSERVATIVE strategy: desactivar para real (tamaños <$2 no son viables)

## Semana 4 — 23/05 → 31/05: Validación pre-lanzamiento
- [ ] 7 días de paper trading con INITIAL_CAPITAL=150 (validar sizing)
- [ ] Test end-to-end en devnet: señal → swap → confirmación → cierre
- [ ] Checklist de go-live (ver abajo)
- [ ] Backup automático de DB (diario)
- [ ] Dashboard: métricas reales (PnL horario, win rate últimos 7 días)

---

## Checklist Go-Live 01/06/2026

### Obligatorio antes de invertir $1 real:
- [ ] 32+ tests pasando
- [ ] Bot corrió 7 días continuos sin crash
- [ ] Ningún bug crítico en los últimos 7 días
- [ ] Retry funcionando en DexScreener y Jupiter
- [ ] Stop loss confirmado funcionando (prueba manual)
- [ ] Capital: $150 configurado, max trade $7.50 (5% Kelly)
- [ ] CONSERVATIVE desactivada para real (tamaños muy chicos)
- [ ] Alerta "bot idle" probada y funcionando en Telegram
- [ ] Backup de DB funcionando
- [ ] .env con PRIVATE_KEY solo legible por root (chmod 600)
- [ ] Inicio gradual: primeros 3 días con $45 (30% del capital)

### Opcional pero recomendable:
- [ ] API key en la API REST
- [ ] Rate limiting en la API
- [ ] Prometheus + Grafana básico

---

## Expectativas financieras honestas con $150

| Escenario | Diario | Mensual |
|-----------|--------|---------|
| Conservador (mediana histórica 3.1% sobre capital desplegado) | ~$1-3 | ~$30-90 |
| Realista (promedio ajustado por slippage real) | ~$3-6 | ~$90-180 |
| Malo (días como 13/04, 26/04, 01/05) | -$10 a -$20 | puede perder |

**El sistema ha demostrado:**
- Racha de 10 días: +$643 sobre base de $1000-3000
- Días individuales malos: hasta -$85 en una sesión
- Win rate histórico: 47.6%

**Con $150 real hay que aceptar:**
- Cada trade STANDARD = $7.50 (5% del capital)
- Un stop loss = -$2.25 por trade
- Un día malo (8 trades perdedores como 01/05) = -$18 teórico
- Slippage real en Solana pump.fun: 2-10% por trade (vs 0.5% simulado)
