import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import select

from app.broker import broker, payments_exchange
from app.config import settings
from app.database import async_session_factory
from app.models import OutboxEvent, OutboxStatus

logger = logging.getLogger("outbox")


async def _publish_batch() -> int:
    """Опубликовать пачку неопубликованных outbox-событий. Возвращает число опубликованных."""
    published = 0
    async with async_session_factory() as session:
        # skip locked позволяет нескольким relay-воркерам работать параллельно и
        # не публиковать одно и то же событие дважды.
        result = await session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.pending)
            .order_by(OutboxEvent.created_at)
            .limit(settings.outbox_batch_size)
            .with_for_update(skip_locked=True)
        )
        events = result.scalars().all()

        for event in events:
            try:
                await broker.publish(
                    event.payload,
                    exchange=payments_exchange,
                    routing_key=settings.routing_new,
                    persist=True,
                    message_id=str(event.id),
                    headers={"x-retry-count": 0, "event-type": event.event_type},
                )
            except Exception:  # noqa: BLE001 - продолжаем публиковать остальные события
                event.retries += 1
                logger.exception("Failed to publish outbox event %s", event.id)
                continue

            event.status = OutboxStatus.published
            event.published_at = datetime.now(UTC)
            published += 1

        await session.commit()
    return published


async def run_outbox_relay(stop_event: asyncio.Event) -> None:
    """Непрерывно ретранслировать outbox-события, пока не выставлен `stop_event`."""
    logger.info("Outbox relay started")
    while not stop_event.is_set():
        try:
            count = await _publish_batch()
            if count:
                logger.info("Published %d outbox event(s)", count)
        except Exception:  # noqa: BLE001 - не даём циклу умереть
            logger.exception("Outbox relay iteration failed")
        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=settings.outbox_poll_interval
            )
        except TimeoutError:
            pass
    logger.info("Outbox relay stopped")
