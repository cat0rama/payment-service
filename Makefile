COMPOSE ?= docker compose
VENV    ?= .venv

PY ?= $(shell command -v python3.12 || command -v python3.13 || command -v python3)

ifeq ($(wildcard $(VENV)/bin/python),)
PYTHON ?= python3
else
PYTHON ?= $(VENV)/bin/python
endif

.DEFAULT_GOAL := help

.PHONY: help
help: ## Показать список команд
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: venv
venv: ## Создать .venv и поставить в него зависимости (далее make test/fmt берут его)
	$(PY) -m venv $(VENV)
	$(VENV)/bin/python -m pip install --upgrade pip
	$(VENV)/bin/python -m pip install -r requirements-dev.txt

.PHONY: install
install: ## Установить/обновить зависимости в текущем окружении (.venv, если есть)
	$(PYTHON) -m pip install -r requirements-dev.txt

.PHONY: test
test: ## Юнит-тесты (быстрые, без Docker)
	$(PYTHON) -m pytest --ignore=tests/integration

.PHONY: test-v
test-v: ## Юнит-тесты подробно (-v)
	$(PYTHON) -m pytest -v --ignore=tests/integration

.PHONY: test-int
test-int: ## Интеграционные тесты (нужен Docker: Postgres + RabbitMQ)
	$(PYTHON) -m pytest tests/integration

.PHONY: test-all
test-all: ## Все тесты (юнит + интеграционные, нужен Docker)
	$(PYTHON) -m pytest

.PHONY: test-cov
test-cov: ## Юнит-тесты с покрытием (нужен pytest-cov)
	$(PYTHON) -m pytest --ignore=tests/integration --cov=app --cov-report=term-missing

.PHONY: fmt
fmt: ## Авто-исправление (ruff check --fix) + форматирование (ruff format)
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

.PHONY: lint
lint: ## Проверка кода линтером (ruff)
	$(PYTHON) -m ruff check .

.PHONY: lint-fix
lint-fix: ## Линт с авто-исправлением, где возможно
	$(PYTHON) -m ruff check --fix .

.PHONY: env
env: ## Создать .env из .env.example, если его ещё нет
	@test -f .env || { cp .env.example .env && echo "Создан .env из .env.example"; }

.PHONY: up
up: env ## Поднять весь стек (api, consumer, БД, брокер, мониторинг)
	$(COMPOSE) up --build

.PHONY: up-d
up-d: env ## Поднять стек в фоне
	$(COMPOSE) up --build -d

.PHONY: down
down: ## Остановить стек
	$(COMPOSE) down

.PHONY: clean
clean: ## Остановить стек и удалить volume'ы (БД, метрики, логи)
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Хвост логов API и consumer
	$(COMPOSE) logs -f api consumer

.PHONY: ps
ps: ## Статус сервисов
	$(COMPOSE) ps

.PHONY: migrate
migrate: ## Применить миграции Alembic вручную
	$(COMPOSE) run --rm migrate alembic upgrade head
