# Асинхронный сервис процессинга платежей

Микросервис принимает запросы на оплату, гарантированно публикует событие через
**Outbox pattern**, асинхронно обрабатывает платёж через эмуляцию платёжного шлюза
и уведомляет клиента о результате через **webhook**. Сообщения, не обработанные
после нескольких попыток, попадают в **Dead Letter Queue**.

## Стек

- **Python 3.12** (в Docker — `python:3.12-slim`; локально подходит **3.12 или 3.13**)
- **FastAPI** 0.115.6 + **Pydantic v2** 2.10.4 (`pydantic-settings` 2.7.1)
- **SQLAlchemy** 2.0.36 (асинхронный режим, `asyncpg` 0.30.0)
- **PostgreSQL** 16
- **RabbitMQ** 3.13 через **FastStream** 0.5.34
- **uvicorn[standard]** 0.34.0, **httpx** 0.28.1
- **Alembic** 1.14.0 (миграции)
- **Docker** + **docker-compose**
- Мониторинг: **Grafana** 11.1.0, **Prometheus** 2.54.1, **Loki/Promtail** 3.1.1
  (метрики — `prometheus-fastapi-instrumentator` 7.0.0)

Полные пины: рантайм — [`requirements.txt`](requirements.txt); инструменты разработки
(pytest 8.3.4, pytest-asyncio 0.25.2, ruff 0.8.6, testcontainers 4.8.2) —
[`requirements-dev.txt`](requirements-dev.txt).

### Требования к версии Python

Локальная установка (`make venv` / `make install`) рассчитана на **Python 3.12 или
3.13**. На **3.14+** ставить нельзя: для запиненных `pydantic-core` и `watchfiles`
ещё нет готовых wheel под 3.14.

## Архитектура

```
                         ┌──────────────────────────────────────────────┐
   POST /payments        │  API (FastAPI)                                │
 ───────────────────────▶│  payments + outbox  (одна транзакция)         │
                         └──────────────────────────────────────────────┘
                                            │
                          Outbox relay (polling, SKIP LOCKED)
                                            │  publish payments(new)
                                            ▼
              ┌──────────────────────────────────────────────────────┐
              │ RabbitMQ                                              │
              │                                                       │
              │  exchange "payments" ──(new)──▶ queue payments.new    │
              │                                      │ обработка       │
              │   при ошибке → publish (retry)       ▼                │
              │  exchange "payments.dlx" ─(retry)─▶ payments.retry     │
              │        (TTL backoff, dead-letter back to payments.new)│
              │                          └─(dead)─▶ payments.dlq (DLQ) │
              └──────────────────────────────────────────────────────┘
                                            │
                            Consumer: эмуляция шлюза (2–5с, 90% успех),
                            обновление статуса в БД, webhook с retry
                                            │
                                            ▼  POST webhook_url
                                       Клиентский сервис
```

### Outbox pattern

При создании платежа в **одной транзакции** пишутся две строки: в `payments`
(статус `pending`) и в `outbox` (событие `payment.created`). Отдельный фоновый
relay (запущен внутри процесса `consumer`) периодически вычитывает неопубликованные
события (`SELECT ... FOR UPDATE SKIP LOCKED`), публикует их в RabbitMQ и помечает
как `published`. Это гарантирует, что событие не потеряется, даже если брокер был
недоступен в момент создания платежа.

### Retry и Dead Letter Queue

- **Webhook**: доставка повторяется до `WEBHOOK_MAX_ATTEMPTS` раз с экспоненциальной
  задержкой внутри обработчика.
- **Обработка сообщения**: при любой ошибке обработки сообщение перекладывается в
  очередь `payments.retry` с per-message TTL (экспоненциальный backoff:
  `RETRY_BACKOFF_BASE_SECONDS * 2^n`). По истечении TTL оно dead-letter'ится обратно
  в `payments.new`. После `MAX_PROCESSING_ATTEMPTS` попыток сообщение отправляется в
  `payments.dlq` (DLQ).

### Идемпотентность

- **На уровне API**: заголовок `Idempotency-Key` уникален в таблице `payments`.
  Повторный запрос с тем же ключом возвращает существующий платёж (`200 OK`) и не
  создаёт дубль и не публикует новое событие.
- **На уровне consumer**: эмуляция шлюза выполняется только пока платёж в статусе
  `pending`; повторная доставка сообщения не приводит к повторной обработке. Webhook
  отправляется только если `webhook_delivered = false`.

## Сети и изоляция (безопасность)

Сервисы разнесены по двум изолированным docker-сетям, чтобы база данных была
недостижима из стека наблюдаемости:

```
        backend (слой данных)                    monitoring (наблюдаемость)
  ┌────────────────────────────────┐      ┌────────────────────────────────┐
  │  postgres   rabbitmq            │      │  grafana   prometheus           │
  │  migrate    consumer            │      │  loki      promtail             │
  │                                 │      │                                 │
  │                      api ◀──────┼──────┼────▶ (Prometheus scrape /metrics)│
  └────────────────────────────────┘ мост └────────────────────────────────┘
```

- **`backend`** — `postgres`, `rabbitmq` и сервисы, которые с ними работают
  (`migrate`, `api`, `consumer`).
- **`monitoring`** — `grafana`, `prometheus`, `loki`, `promtail`.
- **`api`** — единственный сервис в **обеих** сетях: он работает с БД/брокером в
  `backend` и одновременно отдаёт метрики, которые забирает Prometheus в
  `monitoring`. Это единственный мост между сетями.

**Зачем это нужно.** Grafana и весь мониторинг-стек вынесены в отдельную сеть и
**не имеют сетевого маршрута до `postgres`/`rabbitmq`**. Это сделано осознанно
ради безопасности: у Grafana есть встроенный PostgreSQL data source, поэтому на
одной сети с БД любой с доступом к Grafana (а там по умолчанию `admin/admin` и
включён анонимный вход) мог бы подключиться к базе и выполнять SQL. Сегментация
убирает саму возможность: даже при компрометации Grafana (CVE у неё находят
регулярно) порт `postgres:5432` для неё попросту недостижим. Это принцип least
privilege на сетевом уровне — компонент не должен дотягиваться до того, что ему
не нужно.

Проверить изоляцию (после `docker compose up`):

```bash
# Какие сети у каждого контейнера — postgres только в backend, grafana только в monitoring:
docker compose ps -q | xargs docker inspect \
  --format '{{.Name}}: {{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}'

# api в БД достучаться может (общая сеть backend):
docker compose exec api python -c \
  "import socket; socket.create_connection(('postgres',5432),2); print('api -> postgres: OK')"
```

Так как у `grafana` и `postgres` нет ни одной общей сети, имя `postgres` из
контейнера Grafana даже не резолвится — соединение невозможно в принципе.

## Сущность Payment

| Поле | Описание |
|------|----------|
| `id` | UUID, уникальный идентификатор |
| `amount` | сумма (decimal) |
| `currency` | `RUB` / `USD` / `EUR` |
| `description` | описание |
| `metadata` | JSON с доп. информацией |
| `status` | `pending` / `succeeded` / `failed` |
| `idempotency_key` | ключ защиты от дублей (unique) |
| `webhook_url` | URL уведомления о результате |
| `created_at`, `processed_at` | даты создания и обработки |

## Запуск

```bash
cp .env.example .env
docker compose up --build
# или короче через Makefile (make up сам создаёт .env из .env.example, если его нет):
make up
```

Все команды доступны через `make help`. Полезное: `make up` / `make down` /
`make clean` (со сбросом данных), `make logs`, `make migrate`, `make test`.

Поднимаются сервисы приложения — `postgres`, `rabbitmq`, `migrate` (одноразовый
прогон миграций), `api`, `consumer` — и стек наблюдаемости: `loki`, `promtail`,
`prometheus`, `grafana`. Как они разнесены по сетям — см. [Сети и изоляция](#сети-и-изоляция-безопасность).

- API: <http://localhost:8000> — Swagger UI: <http://localhost:8000/docs>
- RabbitMQ Management UI: <http://localhost:15672> (guest / guest)
- Grafana (логи + метрики): <http://localhost:3000> (admin / admin) — дашборды
  «Payments — Logs» (Loki + Promtail) и «Payments — Metrics» (Prometheus).
- Prometheus: <http://localhost:9090>; метрики API: <http://localhost:8000/metrics>
- Логи API-процесса: `GET /logs` (требует `X-API-Key`)

Все эндпоинты требуют заголовок `X-API-Key` (по умолчанию `super-secret-api-key`).

## API — эндпоинты

| Метод | Путь | Авторизация | Назначение |
|-------|------|-------------|------------|
| `POST` | `/api/v1/payments` | `X-API-Key` + `Idempotency-Key` | Создать платёж. `202 Accepted` (новый) или `200 OK` (идемпотентный повтор по тому же ключу). В ответе — заголовки `Idempotency-Key` и `Idempotent-Replayed: true/false`. Невалидный `webhook_url` (SSRF-guard) → `400` |
| `GET` | `/api/v1/payments` | `X-API-Key` | Список обработанных платежей (`succeeded`/`failed`), новые первыми. Query: `limit` (1–1000, по умолч. 100), `offset` (≥0) |
| `GET` | `/api/v1/payments/{payment_id}` | `X-API-Key` | Детали платежа по UUID. `404`, если не найден |
| `GET` | `/health` | — | Liveness-проба, `{"status":"ok"}`. Без ключа и без rate-limit |
| `GET` | `/logs` | `X-API-Key` | Последние записи лога процесса API (in-memory). Query: `limit`, `level` (`DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL`) |
| `GET` | `/metrics` | — | Метрики Prometheus (RPS, латентность, коды ответов). Без ключа и без rate-limit, не входит в OpenAPI |
| `GET` | `/docs`, `/redoc`, `/openapi.json` | — | Swagger UI / ReDoc / схема OpenAPI (FastAPI) |

Все бизнес-эндпоинты (`/api/v1/*` и `/logs`) требуют заголовок `X-API-Key`
(по умолчанию `super-secret-api-key`) и подпадают под rate limiting; `/health` и
`/metrics` — нет. Подробности по аутентификации и лимитам — в [Безопасность](#безопасность).

## Примеры запросов

### Создание платежа

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-api-key" \
  -H "Idempotency-Key: order-12345" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "199.99",
        "currency": "RUB",
        "description": "Premium subscription",
        "metadata": {"order_id": 12345, "user_id": 777},
        "webhook_url": "https://webhook.site/your-uuid"
      }'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "0b3f....",
  "status": "pending",
  "created_at": "2026-06-15T17:35:00.123456+00:00"
}
```

Повторный запрос с тем же `Idempotency-Key` вернёт `200 OK` и тот же `payment_id`.

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: super-secret-api-key"
```

```json
{
  "payment_id": "0b3f....",
  "amount": "199.99",
  "currency": "RUB",
  "description": "Premium subscription",
  "metadata": {"order_id": 12345, "user_id": 777},
  "status": "succeeded",
  "idempotency_key": "order-12345",
  "webhook_url": "https://webhook.site/your-uuid",
  "failure_reason": null,
  "created_at": "2026-06-15T17:35:00.123456+00:00",
  "processed_at": "2026-06-15T17:35:03.456789+00:00"
}
```

### Webhook payload

На `webhook_url` приходит `POST`:

```json
{
  "event": "payment.processed",
  "payment_id": "0b3f....",
  "status": "succeeded",
  "amount": "199.99",
  "currency": "RUB",
  "description": "Premium subscription",
  "metadata": {"order_id": 12345, "user_id": 777},
  "failure_reason": null,
  "processed_at": "2026-06-15T17:35:03.456789+00:00"
}
```

Удобно протестировать на <https://webhook.site>.

### Проверка результата работы

Готовый сценарий (копируется целиком в bash): создаёт платёж, извлекает из
ответа `payment_id` и опрашивает статус. `webhook_url` обязан быть **https** и
публичным (SSRF-guard отклоняет `http` и приватные/loopback хосты) — уникальный
адрес для приёма уведомления выдаёт <https://webhook.site> (кнопка **Copy** у
«Your unique URL»):

```bash
API=http://localhost:8000
KEY=super-secret-api-key
# Уникальный адрес, выданный webhook.site:
WEBHOOK_URL=https://webhook.site/00000000-0000-0000-0000-000000000000

# 1. Создаём платёж и сразу достаём payment_id из ответа 202:
PAYMENT_ID=$(curl -s -X POST "$API/api/v1/payments" \
  -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: order-$(date +%s)" \
  -H "Content-Type: application/json" \
  -d "{
        \"amount\": \"199.99\",
        \"currency\": \"RUB\",
        \"description\": \"Test payment\",
        \"metadata\": {\"order_id\": 12345, \"user_id\": 777},
        \"webhook_url\": \"$WEBHOOK_URL\"
      }" | python3 -c "import sys, json; print(json.load(sys.stdin)['payment_id'])")
echo "payment_id = $PAYMENT_ID"

# 2. Ждём обработку (2–5 c) и смотрим статус: pending -> succeeded/failed,
#    появляется processed_at:
sleep 6
curl -s "$API/api/v1/payments/$PAYMENT_ID" -H "X-API-Key: $KEY" | python3 -m json.tool
```

Остальные эффекты проверяются так:

3. **Webhook** — на указанный `webhook_url` (адрес с `webhook.site`) приходит `POST`
   с финальным статусом и HMAC-подписью в заголовках `X-Webhook-Signature` /
   `X-Webhook-Timestamp`.
4. **Логи и метрики:**
   - логи API-процесса: `curl -s http://localhost:8000/logs -H "X-API-Key: super-secret-api-key"`;
   - метрики: <http://localhost:8000/metrics> или дашборды в Grafana
     (<http://localhost:3000>, «Payments — Logs» и «Payments — Metrics»);
   - очереди и DLQ: RabbitMQ Management UI <http://localhost:15672> (guest / guest).
5. **Идемпотентность** — повторный `POST` с тем же `Idempotency-Key` возвращает
   `200 OK` и тот же `payment_id`, без создания дубля.

Проверить, что всё живо:

```bash
curl -s http://localhost:8000/health      # liveness, без X-API-Key
docker compose ps                         # статусы и healthcheck'и контейнеров
```

## Дашборды Grafana

Дашборды доступны на <http://localhost:3000> сразу после `docker compose up` —
provisioning поднимает их автоматически из `monitoring/grafana/provisioning`.

### Payments — Logs

Централизованные логи всех контейнеров: Promtail собирает stdout через Docker и
шлёт в Loki, Grafana отображает их с фильтром по контейнеру и полнотекстовым
поиском.

![Дашборд Payments — Logs](docs/grafana/logs.png)

### Payments — Metrics

Метрики API из Prometheus (скрейп `api:8000/metrics`), четыре панели:

- **Request rate by status** — частота запросов в разбивке по HTTP-статусу;
- **Request latency (p50 / p95)** — перцентили времени ответа;
- **Payments created (rate)** — частота создания платежей;
- **Rate-limited requests** — запросы, отклонённые rate-limiter'ом (`429`).

![Дашборд Payments — Metrics](docs/grafana/dashboard.png)

## Конфигурация

Переменные окружения (см. `.env.example`):

| Переменная | По умолчанию | Назначение |
|------------|--------------|------------|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@postgres:5432/payments` | строка подключения к БД |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | подключение к RabbitMQ |
| `API_KEY` | `super-secret-api-key` | статический ключ для `X-API-Key` |
| `PROCESSING_SUCCESS_RATE` | `0.9` | доля успешных платежей при эмуляции |
| `WEBHOOK_MAX_ATTEMPTS` | `3` | число попыток доставки webhook |
| `MAX_PROCESSING_ATTEMPTS` | `3` | число попыток обработки до DLQ |
| `WEBHOOK_SIGNING_SECRET` | `change-me-...` | секрет для HMAC-подписи webhook |
| `WEBHOOK_ALLOWED_SCHEMES` | `https` | разрешённые схемы webhook_url (SSRF-guard) |
| `WEBHOOK_ALLOW_PRIVATE_HOSTS` | `false` | разрешить приватные/loopback хосты (только dev) |
| `RATE_LIMIT_ENABLED` | `true` | включить ограничение частоты запросов |
| `RATE_LIMIT_REQUESTS` | `60` | лимит запросов на клиента за окно |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | длина окна лимитирования (сек) |

## Безопасность

- **Аутентификация:** статический `X-API-Key` на всех бизнес-эндпоинтах.
- **Rate limiting:** fixed-window лимит на клиента (по `X-API-Key`, иначе по IP);
  при превышении — `429` с заголовком `Retry-After`. `/health` и `/metrics` не лимитируются.
- **SSRF-защита webhook:** `webhook_url` проверяется при создании платежа (быстрый
  `400`) и повторно перед доставкой — запрещены не-`https` схемы и хосты,
  резолвящиеся в приватные/loopback/link-local адреса (напр. `169.254.169.254`).
- **Подпись webhook:** тело подписывается HMAC-SHA256; получателю приходят заголовки
  `X-Webhook-Signature: t=<ts>,v1=<hmac>` и `X-Webhook-Timestamp`. Проверка на стороне
  клиента: `hmac_sha256(secret, f"{ts}.{raw_body}")`, сравнивать `compare_digest`.

## Тесты

```bash
make install    # pip install -r requirements-dev.txt
make test       # юнит-тесты (быстрые, без Docker)
make test-int   # интеграционные (нужен Docker: Postgres + RabbitMQ)
make test-all   # всё вместе
```

- **Юнит** (`tests/`) не требуют Postgres/RabbitMQ — внешние зависимости замоканы:
  SSRF-guard, HMAC-подпись, rate limiter, retry/DLQ-решение, идемпотентность
  обработки и контракт API (202/200/401).
- **Интеграционные** (`tests/integration/`) поднимают **реальные** Postgres и
  RabbitMQ через `testcontainers` и проверяют стыки: идемпотентность и атомарность
  outbox на уровне БД, публикацию relay в очередь, сохранение результата
  consumer'ом и маршрутизацию в DLQ. Без Docker они автоматически пропускаются.

## Миграции

Миграции применяются автоматически сервисом `migrate` при `docker compose up`.
Вручную:

```bash
docker compose run --rm migrate alembic upgrade head
```

## Структура проекта

```
app/
  main.py        # FastAPI приложение
  config.py      # настройки (pydantic-settings)
  database.py    # async engine / session
  models.py      # SQLAlchemy модели: Payment, OutboxEvent
  schemas.py     # Pydantic v2 схемы
  auth.py        # проверка X-API-Key
  api/payments.py# эндпоинты
  services.py    # создание платежа + запись в outbox (одна транзакция)
  broker.py      # RabbitMQ топология (exchange, очереди, DLQ)
  outbox.py      # relay: публикация событий из outbox
  webhook.py     # доставка webhook с retry + HMAC-подпись
  consumer.py    # FastStream consumer + lifecycle outbox relay
  url_guard.py   # SSRF-проверка webhook_url
  rate_limit.py  # in-process fixed-window rate limiter
  metrics.py     # кастомные Prometheus-метрики
  logging_buffer.py # in-memory буфер логов для GET /logs
alembic/         # миграции
monitoring/      # конфиги Loki, Promtail, Prometheus, провижининг Grafana
tests/           # pytest (юнит-тесты, без внешних зависимостей)
docker-compose.yml
Dockerfile
Makefile         # команды: make up / down / test / logs / migrate
```

Было желание разбить файлы структурно по папкам, но так как их мало, смысла это не имеет.

## Что можно было добавить?
1. JWT для полноценной авторизации.
2. Алерты в телеграм если, сервер пятисотит, если rps(с разных ip) подскакивает экспоненциально.
3. Системные метрики сервера(cpu, memory, swap и тд).
4. Распределённый трейсинг: сквозной `trace_id` через API → очередь → consumer → webhook, чтобы видеть путь одного платежа целиком.
5. Очистку/архивацию опубликованных `outbox` cобытий(сейчас они копятся вечно) и партиционирование `payments` по дате.
6. Ротацию секрета подписи webhook и отдельный эндпоинт для re-drive сообщений из DLQ (переотправка вручную).
7. Метрики из самого `consumer` (доля `failed`, время обработки, длина очередей RabbitMQ) — сейчас Prometheus снимает только с API.
8. CI/CD. прогон линтера и тестов, сборка образов, pre-commit. секреты через Vault вместо plaintext в compose.

## Если сервис высоконагруженный
При больших RPS узкие места — это БД, polling-relay и доставка webhook. Что можно менять:

**База данных**
1.  PgBouncer (пул соединений, защищаем бд) перед Postgres, тюнинг пула, read-реплики под `GET`-запросы.
2.  Партиционирование `payments`/`outbox` по времени + TTL/архивация, чтобы таблицы не пухли.

**Outbox и очередь**
1. При очень высокой пропускной способности - Kafka вместо RabbitMQ(партиции, ordering по ключу платежа, больше пропускная способность).

**Кэш и лимиты**
1. Rate limiter перенести в Redis(сейчас in-process на каждой реплике свой счётчик, для глобального лимита нужен общий стор).
2. Redis-кэш для частых `GET` и горячих справочных данных(Yandex like).

**Наблюдаемость под нагрузкой**
1. Алерты на длину DLQ, лаг outbox(возраст самого старого `pending` события), длину очередей RabbitMQ.
2. Сэмплирование трейсов, чтобы телеметрия сама не стала узким местом.