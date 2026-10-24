import os

# Nombres configurables por variable de entorno (mismos valores en producer, consumer y dlq_consumer)
MAIN_QUEUE = os.getenv("QUEUE_NAME", "ex3_task_queue")
DLX_EXCHANGE = os.getenv("DLX_EXCHANGE", "dlx")
DLQ_QUEUE = os.getenv("DLQ_NAME", "dead_letter_queue")
DLQ_ROUTING_KEY = os.getenv("DLQ_ROUTING_KEY", "dead_letter")


def declare_topology(channel):
    """Declara DLX + DLQ + cola principal.

    Todos los procesos llaman a esta función: las declaraciones son idempotentes, pero
    los argumentos tienen que coincidir exactamente o RabbitMQ responde PRECONDITION_FAILED.
    """
    # 1. Dead Letter Exchange (direct) y la cola donde terminan los mensajes rechazados
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DLQ_QUEUE, durable=True)
    channel.queue_bind(queue=DLQ_QUEUE, exchange=DLX_EXCHANGE, routing_key=DLQ_ROUTING_KEY)

    # 2. Cola principal: todo mensaje rechazado (nack requeue=False), expirado o desbordado
    #    es re-publicado por el broker en el DLX con la routing key indicada.
    channel.queue_declare(
        queue=MAIN_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
        },
    )
