import os
import json
import time
import random
import socket
import signal
import sys
import logging
import pika
from connection import get_rabbitmq_connection
from topology import MAIN_QUEUE, MAX_RETRIES, declare_topology, retry_delays, wait_queue_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("consumer")

# Si se define LOG_FILE, el registro de intentos también se persiste en disco
if os.getenv("LOG_FILE"):
    file_handler = logging.FileHandler(os.environ["LOG_FILE"])
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_handler)

RETRY_COUNT_HEADER = "x-retry-count"
SCHEDULED_AT_HEADER = "x-retry-scheduled-at-ms"
DELAY_HEADER = "x-retry-delay"


def next_action(retry_count, delays):
    """Decide qué hacer con un mensaje que falló.

    `retry_count` es la cantidad de reintentos ya hechos. Devuelve ("retry", delay) mientras
    queden reintentos, o ("dlq", None) cuando ya se hicieron todos.
    """
    if retry_count < len(delays):
        return "retry", delays[retry_count]
    return "dlq", None


def retry_headers(original_headers, retry_count, delay, now):
    headers = {
        k: v for k, v in (original_headers or {}).items()
        # x-death lo administra el broker; no se copia al re-publicar
        if not k.startswith(("x-death", "x-first-death", "x-last-death"))
    }
    headers[RETRY_COUNT_HEADER] = retry_count + 1
    # Los headers AMQP no admiten float: el timestamp viaja como entero en milisegundos
    headers[SCHEDULED_AT_HEADER] = int(now * 1000)
    headers[DELAY_HEADER] = delay
    return headers


class RetryConsumer:
    def __init__(self):
        self.consumer_id = os.getenv("CONSUMER_ID", socket.gethostname())
        self.fail_probability = float(os.getenv("FAIL_PROBABILITY", "0.5"))
        self.processing_time = float(os.getenv("PROCESSING_TIME", "0.2"))
        seed = os.getenv("RANDOM_SEED")
        self.rng = random.Random(int(seed) if seed else None)
        self.delays = retry_delays()
        self.connection = None
        self.channel = None

    def start(self):
        self.connection = get_rabbitmq_connection()
        self.channel = self.connection.channel()
        declare_topology(self.channel)
        # Publisher confirms: basic_publish bloquea hasta que el broker confirma el reintento
        # y lanza excepción si lo rechaza o no lo puede enrutar.
        self.channel.confirm_delivery()
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=MAIN_QUEUE, on_message_callback=self.on_message, auto_ack=False)

        logger.info(
            f"🚀 [CONSUMER:{self.consumer_id}] Escuchando '{MAIN_QUEUE}' | P(fallo)={self.fail_probability} | "
            f"backoff={self.delays}s | luego DLQ"
        )
        try:
            self.channel.start_consuming()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

    def process(self, payload):
        """Simula el procesamiento: falla con probabilidad FAIL_PROBABILITY (o siempre si force_fail)."""
        if self.processing_time > 0:
            time.sleep(self.processing_time)
        if payload.get("force_fail") is True:
            raise RuntimeError("fallo forzado por el productor")
        if self.rng.random() < self.fail_probability:
            raise RuntimeError("fallo transitorio simulado")

    def on_message(self, ch, method, properties, body):
        headers = (properties.headers or {}) if properties else {}
        retry_count = int(headers.get(RETRY_COUNT_HEADER, 0))
        attempt = retry_count + 1
        total_attempts = len(self.delays) + 1

        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            # Un mensaje corrupto no se arregla reintentando: directo a la DLQ
            logger.error(f"❌ [CONSUMER:{self.consumer_id}] Mensaje corrupto ({e}) → DLQ sin reintentos")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        task_id = payload.get("task_id", "desconocido")
        espera = ""
        if SCHEDULED_AT_HEADER in headers:
            waited = time.time() - int(headers[SCHEDULED_AT_HEADER]) / 1000
            espera = f" | esperó {waited:.2f}s (backoff {headers.get(DELAY_HEADER)}s)"
        logger.info(f"📥 [CONSUMER:{self.consumer_id}] Tarea #{task_id} intento {attempt}/{total_attempts}{espera}")

        try:
            self.process(payload)
        except RuntimeError as e:
            self.handle_failure(ch, method, properties, body, task_id, retry_count, e)
            return

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"✅ [CONSUMER:{self.consumer_id}] Tarea #{task_id} OK en el intento {attempt}/{total_attempts}")

    def handle_failure(self, ch, method, properties, body, task_id, retry_count, error):
        attempt = retry_count + 1
        action, delay = next_action(retry_count, self.delays)

        if action == "dlq":
            logger.warning(
                f"🪦 [CONSUMER:{self.consumer_id}] Tarea #{task_id} falló en el intento {attempt} ({error}). "
                f"Agotó {len(self.delays)} reintentos → DLQ"
            )
            # La cola principal tiene x-dead-letter-exchange=dlx: el broker la deriva a la DLQ
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        target = wait_queue_name(delay)
        logger.warning(
            f"🔁 [CONSUMER:{self.consumer_id}] Tarea #{task_id} falló en el intento {attempt} ({error}). "
            f"Reintento {retry_count + 1}/{len(self.delays)} en {delay}s → '{target}'"
        )
        try:
            # Primero se publica el reintento y recién después se hace ack del original:
            # si el proceso muere entre ambos pasos, el mensaje queda duplicado (at-least-once)
            # pero nunca se pierde.
            ch.basic_publish(
                exchange="",
                routing_key=target,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type=properties.content_type if properties else "application/json",
                    headers=retry_headers(properties.headers if properties else None, retry_count, delay, time.time()),
                ),
                mandatory=True,
            )
        except (pika.exceptions.UnroutableError, pika.exceptions.NackError) as e:
            logger.error(f"❌ [CONSUMER:{self.consumer_id}] No se pudo programar el reintento ({e}); se reencola")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def stop(self):
        logger.info(f"🛑 [CONSUMER:{self.consumer_id}] Deteniendo consumidor...")
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
    RetryConsumer().start()
