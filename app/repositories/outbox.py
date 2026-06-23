from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent, OutboxStatus


class OutboxRepository:
    """Доступ к данным transactional outbox."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def fetch_pending_for_update(self, limit: int) -> list[OutboxEvent]:
        """Неопубликованные события под блокировкой ``FOR UPDATE SKIP LOCKED``.

        SKIP LOCKED позволяет нескольким relay-воркерам работать параллельно и не
        брать одно и то же событие дважды.
        """
        result = await self._session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.pending)
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars().all())

    def mark_published(self, event: OutboxEvent) -> None:
        event.status = OutboxStatus.published
        event.published_at = datetime.now(UTC)

    def mark_failed(self, event: OutboxEvent) -> None:
        event.retries += 1
