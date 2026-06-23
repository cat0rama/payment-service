import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment


class PaymentRepository:
    """Доступ к данным платежей.

    Только запросы к БД, без бизнес-логики и без commit — транзакцией управляет
    вызывающий (сервисный слой).
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, payment: Payment) -> None:
        self._session.add(payment)
        # flush, чтобы БД сгенерировала id (он нужен, например, для outbox-события).
        await self._session.flush()

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    async def list_processed(self, limit: int = 100, offset: int = 0) -> list[Payment]:
        """Платежи, которые уже обработаны (succeeded/failed), новые первыми."""
        result = await self._session.execute(
            select(Payment)
            .where(Payment.processed_at.is_not(None))
            .order_by(Payment.processed_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return list(result.scalars().all())
