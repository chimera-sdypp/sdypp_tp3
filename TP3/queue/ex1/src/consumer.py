import os
import json
import time
import socket
import signal
import sys
import logging
import pika
from connection import get_rabbitmq_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("consumer")


class TaskConsumer:
    def __init__(self):
        self.queue_name = os.getenv("QUEUE_NAME", "task_queue")
        # Identificador del consumidor: si viene CONSUMER_ID se usa, sino el hostname del Pod/contenedor
        self.consumer_id = os.getenv("CONSUMER_ID", socket.gethostname())
        self.processing_time = float(os.getenv("PROCESSING_TIME", "0.5"))
        self.connection = None
        self.channel = None
        self.processed_count = 0

    def start(self):
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()

        # Declarar la misma cola durable
        self.channel.queue_declare(queue=self.queue_name, durable=True)

        # FAIR DISPATCH / ROUND-ROBIN JUSTO:
        # prefetch_count=1 le indica a RabbitMQ no entregar un nuevo mensaje a este
        # worker hasta que haya enviado el ACK del anterior.
        self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_message,
            auto_ack=False
        )

        logger.info(f"🚀 [CONSUMER:{self.consumer_id}] Escuchando tareas en cola '{self.queue_name}' (prefetch=1)...")
        try:
            self.channel.start_consuming()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def on_message(self, ch, method, properties, body):
        try:
            payload = json.loads(body.decode("utf-8"))
            task_id = payload.get("task_id", "desconocido")
            title = payload.get("title", "")

            self.processed_count += 1
            logger.info(
                f"📥 [CONSUMER:{self.consumer_id}] Recibida Tarea #{task_id} ('{title}') | "
                f"Total atendidas por este worker: {self.processed_count}"
            )

            # Simulación de cómputo/procesamiento
            if self.processing_time > 0:
                time.sleep(self.processing_time)

            # Confirmación manual de procesamiento exitoso (ACK)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"✅ [CONSUMER:{self.consumer_id}] Tarea #{task_id} procesada y ACK enviado.")

        except Exception as e:
            logger.error(f"❌ [CONSUMER:{self.consumer_id}] Error procesando mensaje: {e}")
            # Rechazar y reencolar si ocurre una excepción inesperada
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def stop(self):
        logger.info(f"🛑 [CONSUMER:{self.consumer_id}] Deteniendo consumidor... Total procesadas: {self.processed_count}")
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
    consumer = TaskConsumer()
    consumer.start()
