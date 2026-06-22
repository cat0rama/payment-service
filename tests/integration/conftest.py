"""Фикстуры интеграционных тестов.

Поднимает реальные Postgres и RabbitMQ через testcontainers (нужен запущенный Docker).
Переменные окружения выставляются ДО импорта любого модуля app, чтобы
`app.config.settings` и `app.database.engine` привязались к контейнерам.

Если Docker недоступен, старт контейнеров падает, и каждый интеграционный тест
пропускается (см. `_require_docker`), а не падает с ошибкой.
"""

import os

import pytest

_DOCKER_AVAILABLE = True
_START_ERROR = ""

try:
    from testcontainers.postgres import PostgresContainer
    from testcontainers.rabbitmq import RabbitMqContainer

    _POSTGRES = PostgresContainer("postgres:16-alpine")
    _RABBITMQ = RabbitMqContainer("rabbitmq:3.13-management-alpine")
    _POSTGRES.start()
    _RABBITMQ.start()

    os.environ["DATABASE_URL"] = (
        f"postgresql+asyncpg://{_POSTGRES.username}:{_POSTGRES.password}"
        f"@{_POSTGRES.get_container_host_ip()}:{_POSTGRES.get_exposed_port(5432)}"
        f"/{_POSTGRES.dbname}"
    )
    os.environ["RABBITMQ_URL"] = (
        f"amqp://guest:guest@{_RABBITMQ.get_container_host_ip()}"
        f":{_RABBITMQ.get_exposed_port(5672)}/"
    )
    # в интеграционных тестах webhook идёт на loopback-сервер или мокается.
    os.environ["WEBHOOK_ALLOW_PRIVATE_HOSTS"] = "true"
except Exception as exc:  # noqa: BLE001 - если docker недоступен, переходим в skip
    _DOCKER_AVAILABLE = False
    _START_ERROR = str(exc)


@pytest.fixture(scope="session", autouse=True)
def _containers_teardown():
    yield
    if _DOCKER_AVAILABLE:
        _POSTGRES.stop()
        _RABBITMQ.stop()


@pytest.fixture(autouse=True)
def _require_docker():
    if not _DOCKER_AVAILABLE:
        pytest.skip(f"Docker/testcontainers unavailable: {_START_ERROR}")


@pytest.fixture(autouse=True)
async def _db_setup(_require_docker):
    """Создать схему и начинать каждый тест с пустых таблиц."""
    from sqlalchemy import text

    import app.models  # noqa: F401 - регистрируем модели в Base.metadata
    from app.database import Base, async_session_factory, engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with async_session_factory() as session:
        await session.execute(text("TRUNCATE payments, outbox CASCADE"))
        await session.commit()
    yield

    await engine.dispose()


@pytest.fixture
async def broker_ready(_db_setup):
    """Подключённый FastStream-брокер с объявленной топологией и очищенными очередями."""
    from app.broker import broker, declare_topology
    from app.config import settings

    await broker.connect()
    await declare_topology()
    await _purge_queues(settings)
    yield broker
    await broker.close()


@pytest.fixture
async def read_queue():
    """Возвращает асинхронный `read(queue_name)`, который достаёт одно сообщение (или None)."""
    import aio_pika

    connections = []

    async def _read(queue_name: str, timeout: float = 5.0):
        conn = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
        connections.append(conn)
        channel = await conn.channel()
        queue = await channel.get_queue(queue_name, ensure=True)
        return await queue.get(timeout=timeout, fail=False)

    yield _read
    for conn in connections:
        await conn.close()


async def _purge_queues(settings) -> None:
    import aio_pika

    conn = await aio_pika.connect_robust(os.environ["RABBITMQ_URL"])
    try:
        channel = await conn.channel()
        for name in (settings.queue_new, settings.queue_retry, settings.queue_dlq):
            try:
                queue = await channel.get_queue(name, ensure=True)
                await queue.purge()
            except Exception:  # noqa: BLE001 - очереди может ещё не быть
                pass
    finally:
        await conn.close()
