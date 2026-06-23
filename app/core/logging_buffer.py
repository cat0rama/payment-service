"""In-memory кольцевой буфер последних записей лога, отдаётся через эндпоинт /logs.

Это лёгкий, локальный для процесса взгляд на логи (только процесс API). Полная,
надёжная агрегация логов со всех сервисов делается через Loki + Promtail +
Grafana (см. docker-compose / monitoring/).
"""

import logging
from collections import deque
from datetime import UTC, datetime
from threading import Lock

from app.core.config import settings


class RingBufferLogHandler(logging.Handler):
    """Хендлер логирования, хранящий последние записи в памяти."""

    def __init__(self, capacity: int) -> None:
        super().__init__()
        self._buffer: deque[dict] = deque(maxlen=capacity)
        self._lock = Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry = {
                "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                entry["exception"] = self.formatException(record.exc_info)
            with self._lock:
                self._buffer.append(entry)
        except Exception:  # логирование не должно ронять приложение
            self.handleError(record)

    def get_records(
        self, limit: int | None = None, level: str | None = None
    ) -> list[dict]:
        with self._lock:
            items = list(self._buffer)
        if level:
            wanted = level.upper()
            items = [item for item in items if item["level"] == wanted]
        if limit is not None:
            items = items[-limit:]
        return items


ring_buffer_handler = RingBufferLogHandler(capacity=settings.log_buffer_capacity)


def setup_log_buffer(level: int = logging.INFO) -> None:
    """Подключить хендлер-буфер к корневому логгеру (идемпотентно)."""
    root = logging.getLogger()
    if ring_buffer_handler not in root.handlers:
        ring_buffer_handler.setLevel(level)
        root.addHandler(ring_buffer_handler)
