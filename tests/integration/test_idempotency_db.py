"""Идемпотентность и атомарность outbox на реальном Postgres."""

from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.database import async_session_factory
from app.models import Currency, OutboxEvent, OutboxStatus, Payment
from app.schemas import PaymentCreate
from app.services import PAYMENT_CREATED_EVENT, PaymentService

pytestmark = pytest.mark.integration


def _data() -> PaymentCreate:
    return PaymentCreate(
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description=None,
        metadata=None,
        webhook_url="https://example.com/h",
    )


async def test_create_persists_payment_and_outbox_in_one_transaction():
    async with async_session_factory() as session:
        payment, created = await PaymentService(session).create_payment(
            _data(), "key-1"
        )

    assert created is True

    async with async_session_factory() as session:
        payments = await session.scalar(select(func.count()).select_from(Payment))
        events = (await session.execute(select(OutboxEvent))).scalars().all()

    assert payments == 1
    assert len(events) == 1
    assert events[0].status == OutboxStatus.pending
    assert events[0].event_type == PAYMENT_CREATED_EVENT
    assert events[0].payload["payment_id"] == str(payment.id)


async def test_same_idempotency_key_does_not_duplicate():
    async with async_session_factory() as session:
        first, created_first = await PaymentService(session).create_payment(
            _data(), "key-2"
        )
    async with async_session_factory() as session:
        second, created_second = await PaymentService(session).create_payment(
            _data(), "key-2"
        )

    assert created_first is True
    assert created_second is False
    assert first.id == second.id

    async with async_session_factory() as session:
        payments = await session.scalar(select(func.count()).select_from(Payment))
        events = await session.scalar(select(func.count()).select_from(OutboxEvent))

    assert payments == 1  # дубля платежа нет
    assert events == 1  # и дубля outbox-события тоже нет
