"""Prometheus-метрики приложения.

Стандартные HTTP-метрики (частота запросов, гистограмма задержек, коды ответов)
добавляет prometheus-fastapi-instrumentator в app.main. Здесь объявлены кастомные
бизнес-метрики. Они регистрируются в реестре по умолчанию и поэтому
автоматически отдаются на эндпоинте /metrics.
"""

from prometheus_client import Counter

payments_created_total = Counter(
    "payments_created_total",
    "Number of payments created via the API (idempotent replays excluded).",
)

rate_limited_total = Counter(
    "payments_rate_limited_total",
    "Number of requests rejected by the rate limiter.",
)
