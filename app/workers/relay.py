import asyncio
import logging

from app.broker import broker, payments_exchange
from app.core.config import settings
from app.db.database import async_session_factory
from app.repositories import OutboxRepository

logger = logging.getLogger("outbox")


class OutboxRelay:
    """Фоновый ретранслятор transactional outbox.

    Периодически вычитывает неопубликованные события из БД (через
    ``OutboxRepository``), публикует их в RabbitMQ и помечает как published.
    Гарантирует доставку события, даже если брокер был недоступен в момент
    создания платежа (at-least-once; дубликаты гасит идемпотентный consumer).
    """

    def __init__(
        self,
        message_broker=broker,
        exchange=payments_exchange,
        session_factory=async_session_factory,
    ) -> None:
        self._broker = message_broker
        self._exchange = exchange
        self._session_factory = session_factory

    async def publish_batch(self) -> int:
        """Опубликовать пачку ожидающих событий. Возвращает число опубликованных."""
        published = 0
        async with self._session_factory() as session:
            repo = OutboxRepository(session)
            events = await repo.fetch_pending_for_update(settings.outbox_batch_size)
            for event in events:
                try:
                    await self._broker.publish(
                        event.payload,
                        exchange=self._exchange,
                        routing_key=settings.routing_new,
                        persist=True,
                        message_id=str(event.id),
                        headers={"x-retry-count": 0, "event-type": event.event_type},
                    )
                except Exception:  # noqa: BLE001 - продолжаем с остальными событиями
                    repo.mark_failed(event)
                    logger.exception("Failed to publish outbox event %s", event.id)
                    continue
                repo.mark_published(event)
                published += 1
            await session.commit()
        return published

    async def run(self, stop_event: asyncio.Event) -> None:
        """Крутить публикацию, пока не выставлен ``stop_event``."""
        logger.info("Outbox relay started")
        while not stop_event.is_set():
            try:
                count = await self.publish_batch()
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
