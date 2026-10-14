import os

# Nombres configurables por variable de entorno (mismos valores en producer, consumer y dlq_consumer)
MAIN_QUEUE = os.getenv("QUEUE_NAME", "ex4_task_queue")
# DLX y DLQ con los mismos nombres y argumentos que el ejemplo 3: es "la DLQ del ejemplo anterior"
DLX_EXCHANGE = os.getenv("DLX_EXCHANGE", "dlx")
DLQ_QUEUE = os.getenv("DLQ_NAME", "dead_letter_queue")
DLQ_ROUTING_KEY = os.getenv("DLQ_ROUTING_KEY", "dead_letter")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
BASE_DELAY_SECONDS = int(os.getenv("BASE_DELAY_SECONDS", "1"))


def retry_delays(max_retries=MAX_RETRIES, base=BASE_DELAY_SECONDS):
    """Backoff exponencial: base * 2^n para cada reintento → [1, 2, 4, 8] con los defaults."""
    return [base * 2 ** n for n in range(max_retries)]


def wait_queue_name(delay_seconds):
    return f"{MAIN_QUEUE}.wait.{delay_seconds}s"


def declare_topology(channel):
    """Declara DLX + DLQ, la cola principal y una cola de espera por cada nivel de backoff.

    Las colas de espera no tienen consumidores: el mensaje queda ahí hasta que vence el
    TTL de la cola (x-message-ttl) y RabbitMQ lo dead-letterea de vuelta a la cola
    principal a través del default exchange ("").
    """
    # 1. DLX + DLQ (idénticos al ejemplo 3)
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DLQ_QUEUE, durable=True)
    channel.queue_bind(queue=DLQ_QUEUE, exchange=DLX_EXCHANGE, routing_key=DLQ_ROUTING_KEY)

    # 2. Cola principal: un nack(requeue=False) la manda a la DLQ (se usa al agotar los reintentos)
    channel.queue_declare(
        queue=MAIN_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
        },
    )

    # 3. Una cola de espera por nivel, con TTL fijo de cola (no por mensaje): si todos los
    #    mensajes de una cola tienen el mismo TTL, el que está primero es siempre el que vence
    #    primero, y no hay bloqueo de cabeza de cola (ver README).
    for delay in retry_delays():
        channel.queue_declare(
            queue=wait_queue_name(delay),
            durable=True,
            arguments={
                "x-message-ttl": delay * 1000,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": MAIN_QUEUE,
            },
        )
