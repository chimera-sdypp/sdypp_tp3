import os
import json
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
logger = logging.getLogger("subscriber")


class NodeSubscriber:
    def __init__(self):
        self.exchange_name = os.getenv("EXCHANGE_NAME", "blockchain_events")
        # Identificador del nodo en la red
        self.node_id = os.getenv("NODE_ID", socket.gethostname())
        self.connection = None
        self.channel = None
        self.received_blocks = []

    def start(self):
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()

        # Asegurar que el exchange fanout existe
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="fanout",
            durable=True
        )

        # Declarar una cola exclusiva y temporal para este nodo
        # Al no especificar nombre, RabbitMQ genera uno único (amq.gen-...)
        # Al ser exclusive=True, la cola se destruye al desconectarse el suscriptor
        result = self.channel.queue_declare(queue="", exclusive=True)
        queue_name = result.method.queue

        # Bindear la cola del nodo al exchange fanout
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=queue_name
        )

        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.on_message,
            auto_ack=True
        )

        logger.info(
            f"🌐 [NODE:{self.node_id}] Nodo suscrito al exchange '{self.exchange_name}' "
            f"con cola temporal '{queue_name}'. Esperando bloques..."
        )

        try:
            self.channel.start_consuming()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def on_message(self, ch, method, properties, body):
        try:
            event = json.loads(body.decode("utf-8"))
            block_idx = event.get("block_index")
            block_hash = event.get("block_hash", "")
            miner = event.get("miner", "desconocido")
            tx_count = event.get("transactions_count", 0)

            self.received_blocks.append(block_idx)

            logger.info(
                f"📦 [NODE:{self.node_id}] ¡NUEVO BLOQUE MINADO RECIBIDO! "
                f"Bloque #{block_idx} | Hash: {block_hash[:16]}... | Minero: {miner} | "
                f"Transacciones: {tx_count} | Total recibidos en este nodo: {len(self.received_blocks)}"
            )
        except Exception as e:
            logger.error(f"❌ [NODE:{self.node_id}] Error procesando evento de bloque: {e}")

    def stop(self):
        logger.info(f"🛑 [NODE:{self.node_id}] Desconectando nodo. Total de bloques procesados: {len(self.received_blocks)}")
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()


def handle_signal(sig, frame):
    logger.info("Señal de terminación recibida. Cerrando nodo...")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    subscriber = NodeSubscriber()
    subscriber.start()
