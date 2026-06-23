import logging
import uuid
from datetime import UTC, datetime

from app.db.database import async_session_factory
from app.db.models import PaymentStatus
from app.gateway import PaymentGateway
from app.repositories import PaymentRepository
from app.webhooks.sender import WebhookSender

logger = logging.getLogger("consumer")


class PaymentProcessor:
    """Обработка одного платежа: вызов шлюза, сохранение результата, webhook.

    Идемпотентна при повторной доставке сообщения: шлюз вызывается только пока
    платёж ``pending``, webhook — только если ещё не доставлен. Зависимости
    (шлюз, отправитель webhook, фабрика сессий) инжектятся — это упрощает тесты.
    """

    def __init__(
        self,
        session_factory=async_session_factory,
        gateway: PaymentGateway | None = None,
        webhook_sender: WebhookSender | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway or PaymentGateway()
        self._webhook = webhook_sender or WebhookSender()

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self._session_factory() as session:
            repo = PaymentRepository(session)
            payment = await repo.get(payment_id)
            if payment is None:
                logger.warning("Payment %s not found, skipping", payment_id)
                return

            # шлюз эмулируем только пока платёж pending: повторная доставка того же
            # сообщения не приведёт ко второй обработке.
            if payment.status == PaymentStatus.pending:
                succeeded = await self._gateway.charge(payment)
                payment.status = (
                    PaymentStatus.succeeded if succeeded else PaymentStatus.failed
                )
                payment.processed_at = datetime.now(UTC)
                if not succeeded:
                    payment.failure_reason = "Payment declined by gateway"
                await session.commit()
                logger.info(
                    "Payment %s processed -> %s", payment_id, payment.status.value
                )

            # webhook шлём ровно один раз: после успешной доставки ставим флаг.
            if not payment.webhook_delivered:
                await self._webhook.deliver(payment)
                payment.webhook_delivered = True
                await session.commit()
