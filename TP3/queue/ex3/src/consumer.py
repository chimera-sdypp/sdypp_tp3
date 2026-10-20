import os
import json
import time
import socket
import signal
import sys
import logging
from connection import get_rabbitmq_connection
from topology import MAIN_QUEUE, DLX_EXCHANGE, declare_topology

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("consumer")


class TaskConsumer:
    def __init__(self):
        self.consumer_id = os.getenv("CONSUMER_ID", socket.gethostname())
        self.processing_time = float(os.getenv("PROCESSING_TIME", "0.5"))
        self.connection = None
        self.channel = None
        self.acked_count = 0
        self.rejected_count = 0

    def start(self):
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()
        declare_topology(self.channel)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=MAIN_QUEUE,
            on_message_callback=self.on_message,
            auto_ack=False
        )

        logger.info(f"🚀 [CONSUMER:{self.consumer_id}] Escuchando '{MAIN_QUEUE}' (DLX='{DLX_EXCHANGE}')...")
        try:
            self.channel.start_consuming()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def on_message(self, ch, method, properties, body):
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            # Un mensaje corrupto nunca va a poder procesarse: reencolarlo (como en el ej. 1)
            # generaría un loop infinito. Se rechaza y el broker lo deriva a la DLQ.
            logger.error(f"❌ [CONSUMER:{self.consumer_id}] Mensaje corrupto ({e}) → nack a DLQ")
            self.reject(ch, method)
            return

        task_id = payload.get("task_id", "desconocido")

        if payload.get("error") is True:
            logger.warning(f"⛔ [CONSUMER:{self.consumer_id}] Tarea #{task_id} con error=true → nack(requeue=False)")
            self.reject(ch, method)
            return

        if self.processing_time > 0:
            time.sleep(self.processing_time)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        self.acked_count += 1
        logger.info(f"✅ [CONSUMER:{self.consumer_id}] Tarea #{task_id} procesada y ACK enviado.")

    def reject(self, ch, method):
        # requeue=False es lo que dispara el dead-lettering: con requeue=True el mensaje
        # volvería a la misma cola y nunca llegaría a la DLQ.
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        self.rejected_count += 1

    def stop(self):
        logger.info(
            f"🛑 [CONSUMER:{self.consumer_id}] Deteniendo... ACK: {self.acked_count} | Rechazados: {self.rejected_count}"
        )
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
    TaskConsumer().start()
