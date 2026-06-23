import asyncio
import logging
import uuid

from faststream import FastStream
from faststream.rabbit import RabbitMessage

from app.broker import (
    broker,
    declare_topology,
    dlx_exchange,
    new_queue,
    payments_exchange,
)
from app.core.config import settings
from app.services import PaymentProcessor
from app.workers.relay import OutboxRelay

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("consumer")

app = FastStream(broker)

# обработка платежа и фоновый relay — отдельные классы сервисного слоя.
processor = PaymentProcessor()
relay = OutboxRelay()


class RetryPolicy:
    """Решает судьбу неудачно обработанного сообщения: повтор или DLQ(смерть).

    При повторе сообщение публикуется в retry-очередь с per-message TTL
    (экспоненциальный backoff); по истечении TTL оно дед-леттерится обратно в
    основную очередь. После ``max_processing_attempts`` попыток — в DLQ.
    """

    def __init__(self, message_broker, dlx) -> None:
        self._broker = message_broker
        self._dlx = dlx

    async def schedule(
        self, body: dict, message: RabbitMessage, error: Exception
    ) -> None:
        retry_count = int(message.headers.get("x-retry-count", 0) or 0)
        attempts_made = retry_count + 1

        if attempts_made >= settings.max_processing_attempts:
            await self._broker.publish(
                body,
                exchange=self._dlx,
                routing_key=settings.routing_dead,
                persist=True,
                headers={
                    "x-retry-count": attempts_made,
                    "x-death-reason": str(error)[:512],
                },
            )
            logger.error(
                "Payment message %s moved to DLQ after %d attempts: %s",
                body.get("payment_id"),
                attempts_made,
                error,
            )
            return

        backoff = settings.retry_backoff_base_seconds * 2**retry_count
        await self._broker.publish(
            body,
            exchange=self._dlx,
            routing_key=settings.routing_retry,
            persist=True,
            expiration=backoff,  # per-message ttl (секунды); дед-леттер обратно в new
            headers={"x-retry-count": attempts_made},
        )
        logger.warning(
            "Payment message %s scheduled for retry %d/%d in %.1fs: %s",
            body.get("payment_id"),
            attempts_made,
            settings.max_processing_attempts,
            backoff,
            error,
        )


retry_policy = RetryPolicy(broker, dlx_exchange)


@broker.subscriber(new_queue, payments_exchange)
async def handle_payment(body: dict, message: RabbitMessage) -> None:
    payment_id_raw = body.get("payment_id")
    try:
        payment_id = uuid.UUID(str(payment_id_raw))
    except (ValueError, TypeError):
        # битое сообщение: сразу в dlq, без повторов.
        logger.error("Invalid payment_id in message: %r", payment_id_raw)
        await broker.publish(
            body,
            exchange=dlx_exchange,
            routing_key=settings.routing_dead,
            persist=True,
            headers={"x-death-reason": "invalid payment_id"},
        )
        return

    try:
        await processor.process(payment_id)
    except Exception as error:  # noqa: BLE001 - переводим в обработку retry/dlq
        logger.exception("Processing failed for payment %s", payment_id)
        await retry_policy.schedule(body, message, error)


# relay крутится фоновой задачей внутри этого же процесса.
_stop_event: asyncio.Event | None = None
_relay_task: asyncio.Task | None = None


@app.after_startup
async def _on_startup() -> None:
    global _stop_event, _relay_task
    await declare_topology()
    _stop_event = asyncio.Event()
    _relay_task = asyncio.create_task(relay.run(_stop_event))
    logger.info("Consumer started; topology declared and outbox relay running")


@app.on_shutdown
async def _on_shutdown() -> None:
    if _stop_event is not None:
        _stop_event.set()
    if _relay_task is not None:
        await _relay_task
    logger.info("Consumer shut down")
