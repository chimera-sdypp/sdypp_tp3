import os
import json
import time
import logging
from datetime import datetime, timezone
import pika
from connection import get_rabbitmq_connection
from topology import MAIN_QUEUE, declare_topology

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("producer")


def build_payload(task_id, error_every):
    """Arma la tarea; cada `error_every` tareas se marca una con "error": true."""
    return {
        "task_id": task_id,
        "title": f"Tarea #{task_id}",
        "error": error_every > 0 and task_id % error_every == 0,
        "created_at": datetime.now(timezone.utc).isoformat()
    }


def publish_tasks():
    num_tasks = int(os.getenv("NUM_TASKS", "10"))
    error_every = int(os.getenv("ERROR_EVERY", "3"))
    delay_between_tasks = float(os.getenv("DELAY_BETWEEN_TASKS", "0.1"))

    connection = get_rabbitmq_connection()
    channel = connection.channel()
    declare_topology(channel)

    logger.info(f"==> Enviando {num_tasks} tareas a '{MAIN_QUEUE}' (1 de cada {error_every} con error=true)...")

    for i in range(1, num_tasks + 1):
        payload = build_payload(i, error_every)
        channel.basic_publish(
            exchange="",
            routing_key=MAIN_QUEUE,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensaje persistente en disco
                content_type="application/json"
            )
        )
        marca = "⚠️  error=true" if payload["error"] else "ok"
        logger.info(f"📤 [PRODUCER] Publicada tarea #{i} ({marca})")
        if delay_between_tasks > 0:
            time.sleep(delay_between_tasks)

    logger.info(f"==> Se completó el envío de las {num_tasks} tareas.")
    connection.close()


if __name__ == "__main__":
    publish_tasks()
