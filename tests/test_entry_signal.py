"""
Tests críticos para compute_entry_signal — la función que decide ENTER/WATCH/EXIT.
Estos tests habrían detectado el bug de TOR y futuros errores de momentum.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from entry_signal import compute_entry_signal


# ── Helpers ──────────────────────────────────────────────────────────────────
def snap(price, buys=10, sells=5):
    """(price_usd, market_cap, volume, buys, sells)"""
    return (price, price * 1_000_000, price * 50_000, buys, sells)


# ── Tests de señal ENTER (momentum >= 90) ────────────────────────────────────

def test_enter_with_strong_momentum():
    """Precio subiendo + aceleración > 5% + buy pressure fuerte → ENTER"""
    history = [
        snap(0.00010, buys=80, sells=10),   # más reciente
        snap(0.00009, buys=60, sells=8),
        snap(0.00008, buys=50, sells=6),
        snap(0.00007, buys=40, sells=5),
    ]
    current = snap(0.00011, buys=90, sells=8)
    signal, momentum = compute_entry_signal(current, history)
    assert signal == 'ENTER', f"Esperaba ENTER, got {signal} (momentum={momentum})"
    assert momentum >= 90

def test_enter_with_pump_burst():
    """4 snapshots consecutivos subiendo → pump burst bonus → ENTER"""
    history = [snap(p, buys=70, sells=10) for p in [0.0009, 0.0008, 0.0007, 0.0006, 0.0005]]
    current = snap(0.0010, buys=80, sells=10)
    signal, momentum = compute_entry_signal(current, history)
    assert signal == 'ENTER'
    assert momentum >= 90

def test_no_enter_without_buy_pressure():
    """Precio sube pero más ventas que compras → no ENTER"""
    history = [snap(0.00009, buys=5, sells=80), snap(0.00008, buys=4, sells=70)]
    current = snap(0.00010, buys=6, sells=90)
    signal, momentum = compute_entry_signal(current, history)
    assert signal != 'ENTER', f"No debería ser ENTER con sell pressure: {signal}"

def test_no_enter_price_falling():
    """Precio cayendo → no ENTER aunque compras sean altas"""
    history = [snap(0.00011, buys=80, sells=5), snap(0.00012, buys=70, sells=4)]
    current = snap(0.00010, buys=85, sells=6)
    signal, momentum = compute_entry_signal(current, history)
    assert signal != 'ENTER'


# ── Tests de señal WATCH ──────────────────────────────────────────────────────

def test_watch_moderate_momentum():
    """Momentum moderado (40-89) → WATCH"""
    history = [snap(0.00009, buys=20, sells=15), snap(0.00008, buys=18, sells=14)]
    current = snap(0.00010, buys=22, sells=16)
    signal, momentum = compute_entry_signal(current, history)
    assert signal == 'WATCH', f"Esperaba WATCH, got {signal} (momentum={momentum})"
    assert 40 <= momentum < 90


# ── Tests de señal EXIT ───────────────────────────────────────────────────────

def test_exit_price_falling_no_buyers():
    """Precio cae y no hay presión compradora → EXIT"""
    history = [snap(0.00011, buys=5, sells=80), snap(0.00012, buys=4, sells=90)]
    current = snap(0.00010, buys=3, sells=100)
    signal, momentum = compute_entry_signal(current, history)
    assert signal == 'EXIT'


# ── Tests de casos límite y robustez ─────────────────────────────────────────

def test_wait_insufficient_history():
    """Menos de 2 snapshots → WAIT (sin crash)"""
    signal, momentum = compute_entry_signal(snap(0.001), [])
    assert signal == 'WAIT'
    assert momentum == 0

def test_wait_single_snapshot():
    signal, momentum = compute_entry_signal(snap(0.001), [snap(0.001)])
    assert signal == 'WAIT'

def test_no_division_by_zero_equal_prices():
    """Precios iguales consecutivos → no ZeroDivisionError"""
    history = [snap(0.001, 10, 10), snap(0.001, 10, 10)]
    current = snap(0.001, 10, 10)
    signal, momentum = compute_entry_signal(current, history)
    assert signal in ('WAIT', 'WATCH', 'EXIT', 'ENTER')

def test_no_crash_zero_buys_sells():
    """0 buys y 0 sells → no crash por división"""
    history = [snap(0.001, 0, 0), snap(0.0009, 0, 0)]
    current = snap(0.0011, 0, 0)
    signal, momentum = compute_entry_signal(current, history)
    assert signal in ('WAIT', 'WATCH', 'EXIT', 'ENTER')

def test_momentum_score_is_non_negative():
    """Momentum nunca debe ser negativo"""
    cases = [
        (snap(0.0005, 0, 0), [snap(0.001, 0, 0), snap(0.0015, 0, 0)]),
        (snap(0.001, 50, 3), [snap(0.0008, 40, 2), snap(0.0006, 30, 1)]),
    ]
    for current, history in cases:
        _, momentum = compute_entry_signal(current, history)
        assert momentum >= 0, f"Momentum negativo: {momentum}"
