import logging
import time
import uuid

from fastapi import Depends, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.payments import router as payments_router
from app.auth import require_api_key
from app.config import settings
from app.logging_buffer import ring_buffer_handler, setup_log_buffer
from app.metrics import rate_limited_total
from app.rate_limit import FixedWindowRateLimiter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
setup_log_buffer()

logger = logging.getLogger("api")

app = FastAPI(
    title="Payments Processing Service",
    version="1.0.0",
    description=(
        "Asynchronous payment processing microservice. Payments are accepted "
        "via the API, published through a transactional outbox to RabbitMQ, "
        "processed by a consumer and the result is delivered via webhook."
    ),
)

# cors включаем только если заданы origin-ы. это API для общения сервер-к-серверу,
# из браузера его обычно не зовут — поэтому по умолчанию cors выключен.
if settings.cors_allow_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# стандартные http-метрики (rps, гистограмма латентности, коды ответов) на /metrics.
Instrumentator().instrument(app).expose(
    app, endpoint="/metrics", include_in_schema=False
)

_rate_limiter = FixedWindowRateLimiter(
    settings.rate_limit_requests, settings.rate_limit_window_seconds
)
# эндпоинты, которые никогда не лимитируются (health и снятие метрик).
_RATE_LIMIT_EXEMPT = {"/health", "/metrics"}


def _client_key(request: Request) -> str:
    """Определить клиента для rate limiting: API-ключ, если есть, иначе IP клиента."""
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return f"key:{api_key}"
    client = request.client
    return f"ip:{client.host}" if client else "ip:unknown"


# объявлен до request_context, чтобы request_context оставался внешним
# middleware и логировал все ответы, включая 429, которые формируются здесь.
@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if settings.rate_limit_enabled and request.url.path not in _RATE_LIMIT_EXEMPT:
        allowed, remaining, retry_after = _rate_limiter.check(_client_key(request))
        if not allowed:
            rate_limited_total.inc()
            retry_after_s = int(retry_after) + 1
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={
                    "Retry-After": str(retry_after_s),
                    "X-RateLimit-Limit": str(settings.rate_limit_requests),
                    "X-RateLimit-Remaining": "0",
                },
            )
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(settings.rate_limit_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        return response
    return await call_next(request)


@app.middleware("http")
async def request_context(request: Request, call_next):
    """Повесить correlation id на каждый запрос, залогировать исход и отдать
    заголовки времени/request-id для трейсинга между сервисами."""
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.exception(
            "%s %s failed after %.1fms [request_id=%s]",
            request.method,
            request.url.path,
            elapsed_ms,
            request_id,
        )
        raise

    elapsed_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time-Ms"] = f"{elapsed_ms:.1f}"
    logger.info(
        "%s %s -> %d in %.1fms [request_id=%s]",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
        request_id,
    )
    return response


app.include_router(payments_router)


@app.get("/health", tags=["system"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get(
    "/logs",
    tags=["system"],
    dependencies=[Depends(require_api_key)],
    summary="Recent in-memory logs of the API process",
)
async def get_logs(
    limit: int = Query(default=100, ge=1, le=settings.log_buffer_capacity),
    level: str | None = Query(
        default=None,
        description="Filter by level: DEBUG/INFO/WARNING/ERROR/CRITICAL",
    ),
) -> dict:
    """Вернуть последние записи лога, накопленные в этом процессе.

    Это быстрый локальный взгляд. Для полной агрегации логов по всем сервисам
    используй Grafana (на базе Loki), которая собирает stdout каждого контейнера.
    """
    records = ring_buffer_handler.get_records(limit=limit, level=level)
    return {"count": len(records), "records": records}
