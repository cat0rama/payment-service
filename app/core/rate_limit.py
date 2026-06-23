"""Небольшой in-process rate limiter с фиксированным окном.

Ключ — на клиента (API-ключ, иначе IP клиента). Он локален для процесса:
при нескольких репликах API каждая держит лимит независимо. Для единого
глобального лимита по репликам это подкладывают Redis — интерфейс намеренно
узкий, чтобы такую замену было легко сделать.
"""

import time
from threading import Lock


class FixedWindowRateLimiter:
    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, tuple[float, int]] = {}
        self._lock = Lock()

    def check(self, key: str) -> tuple[bool, int, float]:
        """Зарегистрировать обращение по ключу ``key``.

        Возвращает ``(allowed, remaining, retry_after_seconds)``.
        """
        now = time.monotonic()
        with self._lock:
            window_start, count = self._hits.get(key, (now, 0))
            if now - window_start >= self.window_seconds:
                window_start, count = now, 0  # окно истекло, сбрасываем
            count += 1
            self._hits[key] = (window_start, count)
            allowed = count <= self.max_requests
            remaining = max(0, self.max_requests - count)
            retry_after = max(0.0, self.window_seconds - (now - window_start))
        return allowed, remaining, retry_after

    def purge_expired(self) -> None:
        """Удалить полностью истёкшие окна, чтобы ограничить расход памяти."""
        now = time.monotonic()
        with self._lock:
            stale = [
                key
                for key, (start, _) in self._hits.items()
                if now - start >= self.window_seconds
            ]
            for key in stale:
                del self._hits[key]
