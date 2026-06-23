from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from app.config import settings

broker = RabbitBroker(settings.rabbitmq_url)

# основной direct-обменник для событий платежей.
payments_exchange = RabbitExchange(
    settings.exchange_name,
    type=ExchangeType.DIRECT,
    durable=True,
)

# dead-letter обменник. сюда попадают и основная очередь (при reject), и
# retry-очередь (по истечении ttl), а оттуда сообщения идут обратно.
dlx_exchange = RabbitExchange(
    settings.dlx_name,
    type=ExchangeType.DIRECT,
    durable=True,
)

# основная рабочая очередь. dead-letter здесь как страховка: если сообщение
# будет отвергнуто (reject/nack), оно уйдёт в dlx с ключом `dead` и попадёт в
# dlq. в штатном режиме consumer перекладывает сообщения сам — см.
# RetryPolicy в app/consumer.py.
new_queue = RabbitQueue(
    settings.queue_new,
    durable=True,
    routing_key=settings.routing_new,
    arguments={
        "x-dead-letter-exchange": settings.dlx_name,
        "x-dead-letter-routing-key": settings.routing_dead,
    },
)

# очередь повторов (задержки). у сообщений тут есть per-message ttl
# (`expiration`); по истечении они дед-леттерятся обратно в основной
# обменник с routing-ключом `new` для следующей попытки обработки.
retry_queue = RabbitQueue(
    settings.queue_retry,
    durable=True,
    routing_key=settings.routing_retry,
    arguments={
        "x-dead-letter-exchange": settings.exchange_name,
        "x-dead-letter-routing-key": settings.routing_new,
    },
)

# dead letter queue: финальная точка для сообщений, исчерпавших попытки.
dlq_queue = RabbitQueue(
    settings.queue_dlq,
    durable=True,
    routing_key=settings.routing_dead,
)


async def declare_topology() -> None:
    """Идемпотентно объявить все обменники, очереди и их привязки."""
    exch = await broker.declare_exchange(payments_exchange)
    dlx = await broker.declare_exchange(dlx_exchange)

    q_new = await broker.declare_queue(new_queue)
    q_retry = await broker.declare_queue(retry_queue)
    q_dlq = await broker.declare_queue(dlq_queue)

    # payments(new) ведёт в основную рабочую очередь
    await q_new.bind(exch, routing_key=settings.routing_new)
    # payments.dlx(retry) ведёт в retry-очередь (с ttl, дед-леттерит обратно в payments(new))
    await q_retry.bind(dlx, routing_key=settings.routing_retry)
    # payments.dlx(dead) ведёт в dead letter queue
    await q_dlq.bind(dlx, routing_key=settings.routing_dead)
