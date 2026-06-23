from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.config import settings


class Base(DeclarativeBase):
    """Декларативная база для всех ORM-моделей."""


def create_engine() -> AsyncEngine:
    """Создать асинхронный engine из настроек (единая точка конфигурации пула)."""
    return create_async_engine(
        settings.database_url,
        echo=False,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )


engine: AsyncEngine = create_engine()

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI-зависимость: сессия без авто-commit (коммитит сервисный слой)."""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
