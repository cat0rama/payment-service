import uuid
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment, PaymentStatus
from app.schemas import PaymentCreate

PAYMENT_CREATED_EVENT = "payment.created"


async def get_payment_by_idempotency_key(
    session: AsyncSession, idempotency_key: str
) -> Payment | None:
    result = await session.execute(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def list_processed_payments(
    session: AsyncSession, limit: int = 100, offset: int = 0
) -> list[Payment]:
    """Вернуть платежи, которые уже обработаны (succeeded или failed),
    самые недавно обработанные первыми."""
    result = await session.execute(
        select(Payment)
        .where(Payment.processed_at.is_not(None))
        .order_by(Payment.processed_at.desc())
        .limit(limit)
        .offset(offset)
    )
    return list(result.scalars().all())


def _build_outbox_event(payment: Payment) -> OutboxEvent:
    return OutboxEvent(
        aggregate_id=payment.id,
        event_type=PAYMENT_CREATED_EVENT,
        payload={
            "payment_id": str(payment.id),
            "idempotency_key": payment.idempotency_key,
            "amount": str(payment.amount),
            "currency": payment.currency.value,
            "webhook_url": payment.webhook_url,
        },
    )


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> tuple[Payment, bool]:
    """Создать платёж вместе с его outbox-событием в одной транзакции.

    Возвращает платёж и флаг, был ли он только что создан
    (False означает, что вернули существующий платёж по тому же ключу идемпотентности).
    """
    existing = await get_payment_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return existing, False

    payment = Payment(
        amount=Decimal(data.amount),
        currency=data.currency,
        description=data.description,
        payment_metadata=data.metadata,
        webhook_url=str(data.webhook_url),
        idempotency_key=idempotency_key,
        status=PaymentStatus.pending,
    )
    session.add(payment)
    # flush, чтобы получить сгенерированный payment.id — он нужен для outbox-события ниже
    await session.flush()

    session.add(_build_outbox_event(payment))

    try:
        await session.commit()
    except IntegrityError:
        # параллельный запрос выиграл гонку за уникальный ключ идемпотентности.
        await session.rollback()
        existing = await get_payment_by_idempotency_key(session, idempotency_key)
        if existing is None:
            raise
        return existing, False

    await session.refresh(payment)
    return payment, True
