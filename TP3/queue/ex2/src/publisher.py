import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone
import pika
from connection import get_rabbitmq_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("publisher")


def calculate_hash(index, prev_hash, timestamp, nonce, miner):
    data = f"{index}{prev_hash}{timestamp}{nonce}{miner}".encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def publish_mined_blocks():
    exchange_name = os.getenv("EXCHANGE_NAME", "blockchain_events")
    num_blocks = int(os.getenv("NUM_BLOCKS", "5"))
    miner_id = os.getenv("MINER_ID", "Miner-Node-Alpha")
    delay = float(os.getenv("DELAY_BETWEEN_BLOCKS", "0.5"))

    connection = get_rabbitmq_connection()
    channel = connection.channel()

    # Declarar el exchange de tipo 'fanout'
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type="fanout",
        durable=True
    )

    logger.info(f"==> Iniciando publicación de {num_blocks} eventos de 'Nuevo Bloque Minado' en exchange FANOUT '{exchange_name}'...")

    previous_hash = "0000000000000000000000000000000000000000000000000000000000000000"

    for i in range(1, num_blocks + 1):
        timestamp = datetime.now(timezone.utc).isoformat()
        nonce = 1000 + i * 42
        block_hash = calculate_hash(i, previous_hash, timestamp, nonce, miner_id)

        block_event = {
            "event_type": "BLOCK_MINED",
            "block_index": i,
            "timestamp": timestamp,
            "miner": miner_id,
            "previous_hash": previous_hash,
            "block_hash": block_hash,
            "difficulty": 4,
            "nonce": nonce,
            "transactions_count": 10 + i
        }

        body = json.dumps(block_event)

        # En exchange fanout, el routing_key se ignora (RabbitMQ lo difunde a todas las colas bindeadas)
        channel.basic_publish(
            exchange=exchange_name,
            routing_key="",
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2
            )
        )

        logger.info(
            f"⛏️  [PUBLISHER] Emitido evento: Bloque #{i} minado | "
            f"Hash: {block_hash[:16]}... | Minero: {miner_id}"
        )

        previous_hash = block_hash

        if delay > 0 and i < num_blocks:
            time.sleep(delay)

    logger.info(f"==> Se publicaron los {num_blocks} bloques exitosamente.")
    connection.close()


if __name__ == "__main__":
    publish_mined_blocks()
