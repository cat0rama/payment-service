"""Outbox-relay публикует ожидающие события в RabbitMQ и помечает их published."""

import json
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.core.config import settings
from app.db.database import async_session_factory
from app.db.models import Currency, OutboxEvent, OutboxStatus
from app.schemas import PaymentCreate
from app.services import PaymentService
from app.workers.relay import OutboxRelay

pytestmark = pytest.mark.integration


async def test_relay_publishes_event_and_message_lands_in_new_queue(
    broker_ready, read_queue
):
    async with async_session_factory() as session:
        payment, _ = await PaymentService(session).create_payment(
            PaymentCreate(
                amount=Decimal("5.00"),
                currency=Currency.USD,
                description=None,
                metadata=None,
                webhook_url="https://example.com/h",
            ),
            "relay-key",
        )

    published = await OutboxRelay().publish_batch()
    assert published == 1

    # событие помечено как published в бд.
    async with async_session_factory() as session:
        event = (await session.execute(select(OutboxEvent))).scalars().one()
    assert event.status == OutboxStatus.published
    assert event.published_at is not None

    # и реальное сообщение пришло в payments.new.
    message = await read_queue(settings.queue_new)
    assert message is not None
    assert json.loads(message.body)["payment_id"] == str(payment.id)
