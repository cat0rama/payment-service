import uuid
from decimal import Decimal

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment, PaymentStatus
from app.repositories import OutboxRepository, PaymentRepository
from app.schemas import PaymentCreate

PAYMENT_CREATED_EVENT = "payment.created"


class PaymentService:
    """Бизнес-логика платежей поверх репозиториев (слоя работы с БД).

    Сам не выполняет SQL — всё через ``PaymentRepository`` / ``OutboxRepository``;
    отвечает за идемпотентность и за то, чтобы платёж и outbox-событие писались в
    ОДНОЙ транзакции.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create_payment(
        self, data: PaymentCreate, idempotency_key: str
    ) -> tuple[Payment, bool]:
        """Создать платёж вместе с outbox-событием в одной транзакции.

        Возвращает ``(payment, created)``; ``created=False`` означает, что вернули
        существующий платёж по тому же ключу идемпотентности.
        """
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
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
        await self._payments.add(payment)  # flush -> сгенерирован payment.id
        await self._outbox.add(self._build_event(payment))

        try:
            await self._session.commit()
        except IntegrityError:
            # параллельный запрос выиграл гонку за уникальный ключ идемпотентности.
            await self._session.rollback()
            existing = await self._payments.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            return existing, False

        await self._session.refresh(payment)
        return payment, True

    async def get_payment(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._payments.get(payment_id)

    async def list_payments(self, limit: int = 100, offset: int = 0) -> list[Payment]:
        return await self._payments.list_processed(limit, offset)

    @staticmethod
    def _build_event(payment: Payment) -> OutboxEvent:
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
