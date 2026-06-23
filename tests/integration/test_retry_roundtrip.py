"""Ретрай: сообщение из retry-очереди по истечении TTL возвращается в payments.new.

Покрывает то, что не проверяли остальные тесты: реальный round-trip отложенного
повтора через per-message TTL + dead-letter обратно в основную очередь, и что
`expiration` трактуется как СЕКУНДЫ (а не миллисекунды).
"""

import asyncio
import json

import pytest

from app import consumer
from app.config import settings

pytestmark = pytest.mark.integration


class FakeMessage:
    def __init__(self, headers: dict) -> None:
        self.headers = headers


async def test_retry_message_returns_to_new_queue_after_ttl(
    broker_ready, read_queue, monkeypatch
):
    # маленький backoff, чтобы тест шёл быстро (и заодно проверяем единицы TTL).
    monkeypatch.setattr(settings, "retry_backoff_base_seconds", 0.5)
    body = {"payment_id": "22222222-2222-2222-2222-222222222222"}
    # x-retry-count=0 -> attempts_made=1 < max -> ветка retry (а не dlq).
    message = FakeMessage({"x-retry-count": 0})

    await consumer.retry_policy.schedule(body, message, Exception("temporary failure"))

    # сразу в payments.new сообщения быть не должно: оно «отлёживается» ~0.5 с в
    # retry-очереди. (если бы expiration был в мс, оно вернулось бы мгновенно.)
    immediate = await read_queue(settings.queue_new, timeout=0.1)
    assert immediate is None

    # после истечения TTL оно дед-леттерится обратно в payments.new.
    await asyncio.sleep(1.0)
    delivered = await read_queue(settings.queue_new, timeout=2.0)
    assert delivered is not None
    assert json.loads(delivered.body)["payment_id"] == body["payment_id"]
    # счётчик попыток увеличился до 1.
    assert int(delivered.headers.get("x-retry-count")) == 1
