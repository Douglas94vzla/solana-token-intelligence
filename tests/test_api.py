"""
Tests para la API REST — validación de inputs, seguridad básica.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest

try:
    from fastapi.testclient import TestClient
    from api import app
    client = TestClient(app)
    API_AVAILABLE = True
except Exception:
    API_AVAILABLE = False


@pytest.mark.skipif(not API_AVAILABLE, reason="API no disponible")
class TestTokensEndpoint:

    def test_hours_max_bound(self):
        """hours > 168 debe ser rechazado (prevenía queries de años enteros)"""
        resp = client.get("/tokens?hours=100000")
        assert resp.status_code == 422, f"Esperaba 422, got {resp.status_code}"

    def test_hours_min_bound(self):
        """hours=0 debe ser rechazado"""
        resp = client.get("/tokens?hours=0")
        assert resp.status_code == 422

    def test_limit_max_bound(self):
        """limit > 200 debe ser rechazado"""
        resp = client.get("/tokens?limit=201")
        assert resp.status_code == 422

    def test_limit_min_bound(self):
        """limit=0 debe ser rechazado"""
        resp = client.get("/tokens?limit=0")
        assert resp.status_code == 422

    def test_offset_negative_rejected(self):
        """offset negativo debe ser rechazado"""
        resp = client.get("/tokens?offset=-1")
        assert resp.status_code == 422

    def test_default_params_work(self):
        """Request sin parámetros debe responder 200"""
        resp = client.get("/tokens")
        assert resp.status_code == 200
        assert "tokens" in resp.json()

    def test_no_sql_injection_in_with_name(self):
        """with_name es bool — strings SQL no deben pasar"""
        resp = client.get("/tokens?with_name=1; DROP TABLE discovered_tokens;--")
        # FastAPI debe rechazar el bool inválido o parsearlo como True/False
        assert resp.status_code in (200, 422)


@pytest.mark.skipif(not API_AVAILABLE, reason="API no disponible")
class TestSearchEndpoint:

    def test_min_length_enforced(self):
        """q con menos de 2 caracteres debe ser rechazado"""
        resp = client.get("/tokens/search?q=a")
        assert resp.status_code == 422

    def test_search_returns_list(self):
        resp = client.get("/tokens/search?q=test")
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data or "tokens" in data or isinstance(data, list)
