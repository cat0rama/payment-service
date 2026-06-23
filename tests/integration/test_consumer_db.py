"""Consumer сохраняет результат обработки в реальный Postgres (идемпотентно).

PaymentProcessor вызывается с детерминированным шлюзом (без задержки, всегда
успех) и фейковым отправителем webhook — реальный HTTP не нужен, проверяем БД.
"""

import uuid
from decimal import Decimal

import pytest

from app.database import async_session_factory
from app.models import Currency, Payment, PaymentStatus
from app.services import PaymentProcessor

pytestmark = pytest.mark.integration


class FakeGateway:
    def __init__(self, succeed: bool = True) -> None:
        self.succeed = succeed

    async def charge(self, payment) -> bool:
        return self.succeed


class RecordingWebhook:
    def __init__(self) -> None:
        self.calls: list = []

    async def deliver(self, payment) -> None:
        self.calls.append(payment)


async def test_process_payment_persists_succeeded_status():
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

    webhook = RecordingWebhook()
    processor = PaymentProcessor(
        gateway=FakeGateway(succeed=True), webhook_sender=webhook
    )
    await processor.process(payment_id)

    async with async_session_factory() as session:
        stored = await session.get(Payment, payment_id)
    assert stored.status == PaymentStatus.succeeded
    assert stored.processed_at is not None
    assert stored.webhook_delivered is True
    assert len(webhook.calls) == 1


async def test_process_payment_is_idempotent_on_redelivery():
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

    webhook = RecordingWebhook()
    processor = PaymentProcessor(gateway=FakeGateway(), webhook_sender=webhook)
    await processor.process(payment_id)

    assert webhook.calls == []  # повторная доставка не должна слать webhook заново
