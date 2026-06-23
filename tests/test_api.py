"""Контрактные тесты API: коды идемпотентности и авторизация.

Внешние зависимости (сессия БД, DNS-резолв SSRF, сервисный слой) подменены,
поэтому тесты идут без Postgres/RabbitMQ.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from app.api import payments as payments_api
from app.core.config import settings
from app.db.database import get_session
from app.db.models import Currency, Payment, PaymentStatus
from app.main import app


def make_payment() -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("199.99"),
        currency=Currency.RUB,
        description=None,
        payment_metadata=None,
        status=PaymentStatus.pending,
        idempotency_key="order-1",
        webhook_url="https://example.com/h",
        failure_reason=None,
        created_at=datetime.now(UTC),
        processed_at=None,
    )


def _body() -> dict:
    return {
        "amount": "199.99",
        "currency": "RUB",
        "webhook_url": "https://example.com/h",
    }


@pytest.fixture
def client(monkeypatch):
    async def _fake_session():
        yield None

    async def _fake_validate(url, **kwargs):
        return None

    app.dependency_overrides[get_session] = _fake_session
    monkeypatch.setattr(payments_api, "validate_webhook_url_async", _fake_validate)
    monkeypatch.setattr(settings, "rate_limit_enabled", False)
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_create_returns_202(client, monkeypatch):
    async def fake_create(self, data, key):
        return make_payment(), True

    monkeypatch.setattr(payments_api.PaymentService, "create_payment", fake_create)
    resp = client.post(
        "/api/v1/payments",
        json=_body(),
        headers={"X-API-Key": settings.api_key, "Idempotency-Key": "order-1"},
    )
    assert resp.status_code == 202
    assert resp.json()["status"] == "pending"
    assert resp.headers["Idempotency-Key"] == "order-1"
    assert resp.headers["Idempotent-Replayed"] == "false"


def test_idempotent_replay_returns_200(client, monkeypatch):
    async def fake_create(self, data, key):
        return make_payment(), False  # существующий платёж, не созданный заново

    monkeypatch.setattr(payments_api.PaymentService, "create_payment", fake_create)
    resp = client.post(
        "/api/v1/payments",
        json=_body(),
        headers={"X-API-Key": settings.api_key, "Idempotency-Key": "order-1"},
    )
    assert resp.status_code == 200
    assert resp.headers["Idempotency-Key"] == "order-1"
    assert resp.headers["Idempotent-Replayed"] == "true"


def test_missing_api_key_returns_401(client):
    resp = client.post(
        "/api/v1/payments",
        json=_body(),
        headers={"Idempotency-Key": "order-1"},
    )
    assert resp.status_code == 401
