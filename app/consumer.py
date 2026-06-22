import asyncio
import logging
import random
import uuid
from datetime import UTC, datetime

from faststream import FastStream
from faststream.rabbit import RabbitMessage

from app.broker import (
    broker,
    declare_topology,
    dlx_exchange,
    new_queue,
    payments_exchange,
)
from app.config import settings
from app.database import async_session_factory
from app.models import Payment, PaymentStatus
from app.outbox import run_outbox_relay
from app.webhook import deliver_webhook

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("consumer")

app = FastStream(broker)

# relay крутится фоновой задачей внутри этого же процесса. держим ссылки на
# задачу и на сигнал остановки, чтобы при выключении погасить его аккуратно.
_stop_event: asyncio.Event | None = None
_relay_task: asyncio.Task | None = None


async def _process_payment(payment_id: uuid.UUID) -> None:
    """Обработать один платёж: эмулировать вызов шлюза, сохранить результат
    и доставить webhook. Идемпотентно при повторных доставках."""
    async with async_session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            logger.warning("Payment %s not found, skipping", payment_id)
            return

        # шлюз эмулируем только пока платёж pending: если это же сообщение
        # доставят повторно, второй раз обрабатывать платёж мы не станем.
        if payment.status == PaymentStatus.pending:
            delay = random.uniform(
                settings.processing_min_seconds, settings.processing_max_seconds
            )
            await asyncio.sleep(delay)

            succeeded = random.random() < settings.processing_success_rate
            payment.status = (
                PaymentStatus.succeeded if succeeded else PaymentStatus.failed
            )
            payment.processed_at = datetime.now(UTC)
            if not succeeded:
                payment.failure_reason = "Payment declined by gateway"
            await session.commit()
            logger.info(
                "Payment %s processed in %.2fs -> %s",
                payment_id,
                delay,
                payment.status.value,
            )

        # webhook шлём ровно один раз: после успешной доставки ставим флаг,
        # и повторная доставка сообщения уже не продублирует уведомление.
        if not payment.webhook_delivered:
            await deliver_webhook(payment)
            payment.webhook_delivered = True
            await session.commit()


async def _schedule_retry_or_dlq(
    body: dict, message: RabbitMessage, error: Exception
) -> None:
    """Либо переложить сообщение в очередь с экспоненциальным backoff, либо
    отправить его в DLQ, когда попытки обработки исчерпаны."""
    retry_count = int(message.headers.get("x-retry-count", 0) or 0)
    attempts_made = retry_count + 1

    if attempts_made >= settings.max_processing_attempts:
        await broker.publish(
            body,
            exchange=dlx_exchange,
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
    await broker.publish(
        body,
        exchange=dlx_exchange,
        routing_key=settings.routing_retry,
        persist=True,
        expiration=backoff,  # per-message ttl (секунды); дед-леттерит обратно в основную очередь
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
        await _process_payment(payment_id)
    except Exception as error:  # noqa: BLE001 - переводим в обработку retry/dlq
        logger.exception("Processing failed for payment %s", payment_id)
        await _schedule_retry_or_dlq(body, message, error)


@app.after_startup
async def _on_startup() -> None:
    global _stop_event, _relay_task
    await declare_topology()
    _stop_event = asyncio.Event()
    _relay_task = asyncio.create_task(run_outbox_relay(_stop_event))
    logger.info("Consumer started; topology declared and outbox relay running")


@app.on_shutdown
async def _on_shutdown() -> None:
    global _relay_task
    if _stop_event is not None:
        _stop_event.set()
    if _relay_task is not None:
        await _relay_task
    logger.info("Consumer shut down")
