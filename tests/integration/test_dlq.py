"""Исчерпавшие попытки сообщения уходят в Dead Letter Queue через реальный брокер."""

import json

import pytest

from app.config import settings
from app.consumer import _schedule_retry_or_dlq

pytestmark = pytest.mark.integration


class FakeMessage:
    def __init__(self, headers: dict) -> None:
        self.headers = headers


async def test_message_lands_in_dlq_after_max_attempts(broker_ready, read_queue):
    body = {"payment_id": "11111111-1111-1111-1111-111111111111"}
    # retry_count = max-1, значит attempts_made == max, ветка dlq.
    message = FakeMessage({"x-retry-count": settings.max_processing_attempts - 1})

    await _schedule_retry_or_dlq(body, message, Exception("permanent failure"))

    delivered = await read_queue(settings.queue_dlq)
    assert delivered is not None
    assert json.loads(delivered.body)["payment_id"] == body["payment_id"]
    assert delivered.headers.get("x-death-reason")
