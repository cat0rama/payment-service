"""Маршрутизация retry/DLQ (RetryPolicy) и идемпотентная обработка (PaymentProcessor)."""

import uuid
from decimal import Decimal

from app import consumer
from app.config import settings
from app.models import Currency, Payment, PaymentStatus
from app.services import PaymentProcessor


class FakeMessage:
    def __init__(self, headers: dict) -> None:
        self.headers = headers


class FakeSession:
    def __init__(self, payment: Payment) -> None:
        self._payment = payment
        self.commits = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, model, pk):
        return self._payment

    async def commit(self):
        self.commits += 1


class FakeGateway:
    def __init__(self, succeed: bool = True) -> None:
        self.succeed = succeed
        self.charged = False

    async def charge(self, payment) -> bool:
        self.charged = True
        return self.succeed


class FakeWebhook:
    def __init__(self) -> None:
        self.delivered: list = []

    async def deliver(self, payment) -> None:
        self.delivered.append(payment)


async def test_schedule_retry_uses_retry_routing(monkeypatch):
    calls = []

    async def fake_publish(body, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(consumer.broker, "publish", fake_publish)
    monkeypatch.setattr(settings, "max_processing_attempts", 3)

    await consumer.retry_policy.schedule(
        {"payment_id": "p"}, FakeMessage({"x-retry-count": 0}), Exception("boom")
    )

    assert calls[0]["routing_key"] == settings.routing_retry
    assert "expiration" in calls[0]  # per-message ttl backoff


async def test_schedule_moves_to_dlq_after_max(monkeypatch):
    calls = []

    async def fake_publish(body, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(consumer.broker, "publish", fake_publish)
    monkeypatch.setattr(settings, "max_processing_attempts", 3)

    # retry_count=2, значит attempts_made=3 >= max, уходит в dlq
    await consumer.retry_policy.schedule(
        {"payment_id": "p"}, FakeMessage({"x-retry-count": 2}), Exception("boom")
    )

    assert calls[0]["routing_key"] == settings.routing_dead


async def test_process_payment_is_idempotent_when_done():
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("1.00"),
        currency=Currency.RUB,
        status=PaymentStatus.succeeded,
        webhook_delivered=True,
        idempotency_key="k",
        webhook_url="https://127.0.0.1/h",
    )
    gateway = FakeGateway()
    webhook = FakeWebhook()
    processor = PaymentProcessor(
        session_factory=lambda: FakeSession(payment),
        gateway=gateway,
        webhook_sender=webhook,
    )

    await processor.process(payment.id)

    assert gateway.charged is False  # уже обработан, шлюз не дёргаем
    assert webhook.delivered == []  # уже доставлен, повторно не отправляем
    assert payment.status == PaymentStatus.succeeded


async def test_process_payment_runs_when_pending():
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("1.00"),
        currency=Currency.RUB,
        status=PaymentStatus.pending,
        webhook_delivered=False,
        idempotency_key="k2",
        webhook_url="https://127.0.0.1/h",
    )
    gateway = FakeGateway(succeed=True)
    webhook = FakeWebhook()
    processor = PaymentProcessor(
        session_factory=lambda: FakeSession(payment),
        gateway=gateway,
        webhook_sender=webhook,
    )

    await processor.process(payment.id)

    assert gateway.charged is True
    assert payment.status == PaymentStatus.succeeded
    assert payment.webhook_delivered is True
    assert webhook.delivered == [payment]
