"""Consumer сохраняет результат обработки в реальный Postgres (идемпотентно)."""

import uuid
from decimal import Decimal

import pytest

from app import consumer
from app.database import async_session_factory
from app.models import Currency, Payment, PaymentStatus

pytestmark = pytest.mark.integration


async def test_process_payment_persists_succeeded_status(monkeypatch):
    payment_id = uuid.uuid4()
    async with async_session_factory() as session:
        session.add(
            Payment(
                id=payment_id,
                amount=Decimal("1.00"),
                currency=Currency.RUB,
                status=PaymentStatus.pending,
                idempotency_key="proc-1",
                webhook_url="https://127.0.0.1/h",
                webhook_delivered=False,
            )
        )
        await session.commit()

    # детерминированно и быстро: без реальной задержки шлюза, всегда успех, без реального http.
    monkeypatch.setattr(consumer.random, "uniform", lambda a, b: 0.0)
    monkeypatch.setattr(consumer.random, "random", lambda: 0.0)

    async def fake_deliver(p):
        return None

    monkeypatch.setattr(consumer, "deliver_webhook", fake_deliver)

    await consumer._process_payment(payment_id)

    async with async_session_factory() as session:
        stored = await session.get(Payment, payment_id)
    assert stored.status == PaymentStatus.succeeded
    assert stored.processed_at is not None
    assert stored.webhook_delivered is True


async def test_process_payment_is_idempotent_on_redelivery(monkeypatch):
    payment_id = uuid.uuid4()
    async with async_session_factory() as session:
        session.add(
            Payment(
                id=payment_id,
                amount=Decimal("1.00"),
                currency=Currency.RUB,
                status=PaymentStatus.succeeded,  # уже обработан
                idempotency_key="proc-2",
                webhook_url="https://127.0.0.1/h",
                webhook_delivered=True,  # уже доставлен
            )
        )
        await session.commit()

    calls = []

    async def fake_deliver(p):
        calls.append(p)

    monkeypatch.setattr(consumer, "deliver_webhook", fake_deliver)

    await consumer._process_payment(payment_id)

    assert calls == []  # повторная доставка не должна слать webhook заново
