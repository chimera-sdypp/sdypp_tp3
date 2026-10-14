import os
import json
import time
import logging
from datetime import datetime, timezone
import pika
from connection import get_rabbitmq_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("producer")


def publish_tasks():
    queue_name = os.getenv("QUEUE_NAME", "task_queue")
    num_tasks = int(os.getenv("NUM_TASKS", "10"))
    delay_between_tasks = float(os.getenv("DELAY_BETWEEN_TASKS", "0.1"))

    connection = get_rabbitmq_connection()
    channel = connection.channel()

    # Asegurar que la cola es durable (persiste a reinicios de RabbitMQ)
    channel.queue_declare(queue=queue_name, durable=True)

    logger.info(f"==> Iniciando envío de {num_tasks} tareas a la cola '{queue_name}'...")

    for i in range(1, num_tasks + 1):
        payload = {
            "task_id": i,
            "title": f"Tarea #{i}",
            "description": f"Cálculo o procesamiento distribuido correspondiente a la tarea {i}",
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        body = json.dumps(payload)

        channel.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensaje persistente en disco
                content_type="application/json"
            )
        )
        logger.info(f"📤 [PRODUCER] Publicada tarea #{i}: {payload['title']}")
        if delay_between_tasks > 0:
            time.sleep(delay_between_tasks)

    logger.info(f"==> Se completó el envío de las {num_tasks} tareas exitosamente.")
    connection.close()


if __name__ == "__main__":
    publish_tasks()
