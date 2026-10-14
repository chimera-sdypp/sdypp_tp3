import os
import socket
import signal
import sys
import logging
from connection import get_rabbitmq_connection
from topology import DLQ_QUEUE, declare_topology

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("dlq-consumer")


def describe_death(properties):
    """Resume el header x-death que agrega RabbitMQ al dead-letterear un mensaje.

    Es la evidencia de que el desvío lo hizo el broker: indica el motivo (rejected,
    expired, maxlen...), la cola de origen y cuántas veces murió el mensaje ahí.
    """
    headers = (properties.headers or {}) if properties else {}
    deaths = headers.get("x-death") or []
    if not deaths:
        return "sin header x-death"
    last = deaths[0]  # RabbitMQ deja primero el evento más reciente
    retries = headers.get("x-retry-count", 0)
    return (
        f"reason={last.get('reason')} queue={last.get('queue')} count={last.get('count')} "
        f"| reintentos={retries}"
    )


class DeadLetterConsumer:
    def __init__(self):
        self.consumer_id = os.getenv("CONSUMER_ID", socket.gethostname())
        self.connection = None
        self.channel = None
        self.received_count = 0

    def start(self):
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()
        declare_topology(self.channel)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=DLQ_QUEUE, on_message_callback=self.on_message, auto_ack=False)

        logger.info(f"🪦 [DLQ:{self.consumer_id}] Escuchando mensajes fallidos en '{DLQ_QUEUE}'...")
        try:
            self.channel.start_consuming()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def on_message(self, ch, method, properties, body):
        self.received_count += 1
        logger.info(
            f"🪦 [DLQ:{self.consumer_id}] Mensaje fallido #{self.received_count}: "
            f"{body.decode('utf-8', errors='replace')} | {describe_death(properties)}"
        )
        # Acá se podría alertar, persistir o reprocesar; en el ejemplo solo se registra.
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def stop(self):
        logger.info(f"🛑 [DLQ:{self.consumer_id}] Deteniendo... Total mensajes fallidos: {self.received_count}")
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()


def handle_signal(sig, frame):
    logger.info("Señal de terminación recibida. Cerrando ordenadamente...")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    DeadLetterConsumer().start()
