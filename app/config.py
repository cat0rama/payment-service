import json
from functools import lru_cache
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Конфигурация приложения, загружается из переменных окружения."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # postgresql (асинхронный драйвер)
    database_url: str = "postgresql+asyncpg://payments:payments@postgres:5432/payments"

    # rabbitmq
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"

    # статический api-ключ, нужен в заголовке X-API-Key на всех эндпоинтах
    api_key: str = "super-secret-api-key"

    # cors: разрешённые браузерные origin-ы. по умолчанию пусто, это сервер к серверу
    # api, поэтому cors нужен только если api зовут напрямую из браузера.
    # задаётся списком через запятую в переменной окружения CORS_ALLOW_ORIGINS.
    cors_allow_origins: Annotated[list[str], NoDecode] = []

    # сколько последних записей лога держим в памяти для эндпоинта /logs.
    log_buffer_capacity: int = 1000

    # ограничение частоты (фиксированное окно на клиента, ключ - api-ключ или ip).
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 60
    rate_limit_window_seconds: float = 60.0

    # интервал опроса outbox-relay (секунды) и размер пачки
    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 50

    # эмуляция обработки платежа
    processing_min_seconds: float = 2.0
    processing_max_seconds: float = 5.0
    processing_success_rate: float = 0.9

    # политика повторов доставки webhook (попытки внутри обработчика consumer)
    webhook_timeout_seconds: float = 10.0
    webhook_max_attempts: int = 3
    webhook_backoff_base_seconds: float = 1.0

    # безопасность webhook
    # секрет для hmac-подписи тела webhook (заголовок X-Webhook-Signature).
    webhook_signing_secret: str = "change-me-webhook-signing-secret"
    # разрешённые схемы url для webhook_url (ssrf-guard). по умолчанию только https.
    webhook_allowed_schemes: Annotated[set[str], NoDecode] = {"https"}
    # разрешать приватные, loopback и внутренние хосты как цель webhook. в
    # проде держим False, включать только для локальной разработки и тестов.
    webhook_allow_private_hosts: bool = False

    # политика повторов обработки сообщения и dlq
    max_processing_attempts: int = 3
    retry_backoff_base_seconds: float = 2.0

    # имена топологии rabbitmq
    exchange_name: str = "payments"
    dlx_name: str = "payments.dlx"
    queue_new: str = "payments.new"
    queue_retry: str = "payments.retry"
    queue_dlq: str = "payments.dlq"
    routing_new: str = "new"
    routing_retry: str = "retry"
    routing_dead: str = "dead"

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def _split_origins(cls, value: object) -> object:
        # NoDecode выше отключает json-парсинг pydantic-settings, поэтому строку
        # из env разбираем сами: и формат "через запятую", и json-список.
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return []
            if value.startswith("["):
                return json.loads(value)
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @field_validator("webhook_allowed_schemes", mode="before")
    @classmethod
    def _split_schemes(cls, value: object) -> object:
        # NoDecode отключает json-парсинг pydantic-settings — разбираем сами:
        # строка "через запятую" ("https,http") или json-список.
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return set()
            if value.startswith("["):
                return set(json.loads(value))
            return {s.strip().lower() for s in value.split(",") if s.strip()}
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
