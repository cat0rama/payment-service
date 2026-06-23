"""Слой доступа к данным (repository pattern).

Репозитории инкапсулируют ВСЮ работу с БД (SQLAlchemy-запросы). Бизнес-логика
(сервисы, воркеры) обращается к репозиториям, а не к сессии напрямую — так
доступ к данным отделён от логики и легко мокается в тестах.
"""

from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository

__all__ = ["OutboxRepository", "PaymentRepository"]
