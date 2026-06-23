"""Тесты HMAC-подписи webhook и SSRF-проверки при доставке."""

import hashlib
import hmac
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from app.core.config import settings
from app.db.models import Currency, Payment, PaymentStatus
from app.webhooks import sender as webhook
from app.webhooks.url_guard import UnsafeWebhookURL


def make_payment(url: str = "https://127.0.0.1/hook") -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("199.99"),
        currency=Currency.RUB,
        description="x",
        payment_metadata={"order_id": 1},
        status=PaymentStatus.succeeded,
        idempotency_key="k",
        webhook_url=url,
        webhook_delivered=False,
        failure_reason=None,
        processed_at=datetime(2026, 6, 17, tzinfo=UTC),
    )


def test_sign_payload_matches_reference(monkeypatch):
    monkeypatch.setattr(settings, "webhook_signing_secret", "topsalamaleikum")
    body = b'{"hello":"world"}'
    ts = 1700000000
    expected = hmac.new(
        b"topsalamaleikum", b"1700000000." + body, hashlib.sha256
    ).hexdigest()
    assert webhook.sign_payload(body, ts) == expected


async def test_delivery_sends_valid_signature(monkeypatch):
    monkeypatch.setattr(settings, "webhook_allow_private_hosts", True)
    monkeypatch.setattr(settings, "webhook_signing_secret", "s3cr3t")

    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = request.headers
        captured["content"] = request.content
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    real_async_client = httpx.AsyncClient

    def client_factory(*args, **kwargs):
        kwargs["transport"] = transport
        return real_async_client(*args, **kwargs)

    monkeypatch.setattr(webhook.httpx, "AsyncClient", client_factory)

    await webhook.WebhookSender().deliver(make_payment("https://127.0.0.1/hook"))

    ts = int(captured["headers"]["X-Webhook-Timestamp"])
    expected = hmac.new(
        b"s3cr3t", f"{ts}.".encode() + captured["content"], hashlib.sha256
    ).hexdigest()
    assert captured["headers"]["X-Webhook-Signature"] == f"t={ts},v1={expected}"


async def test_delivery_rejects_private_host_when_not_allowed(monkeypatch):
    monkeypatch.setattr(settings, "webhook_allow_private_hosts", False)
    with pytest.raises(UnsafeWebhookURL):
        await webhook.WebhookSender().deliver(make_payment("https://127.0.0.1/hook"))
