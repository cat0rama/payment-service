"""RabbitMQ: брокер, обменники, очереди и объявление топологии."""

from app.broker.connection import (
    broker,
    declare_topology,
    dlq_queue,
    dlx_exchange,
    new_queue,
    payments_exchange,
    retry_queue,
)

__all__ = [
    "broker",
    "declare_topology",
    "dlq_queue",
    "dlx_exchange",
    "new_queue",
    "payments_exchange",
    "retry_queue",
]
