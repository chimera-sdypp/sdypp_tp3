import os
import time
import logging
import pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("rabbitmq-connection")


def get_rabbitmq_connection(max_retries=15, retry_interval=2):
    """Establece conexión con RabbitMQ con reintentos para entornos distribuidos / contenedores."""
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASS", "guest")

    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Intentando conectar a RabbitMQ en {host}:{port} (intento {attempt}/{max_retries})...")
            connection = pika.BlockingConnection(parameters)
            logger.info("Conectado exitosamente a RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError as err:
            logger.warning(f"RabbitMQ no disponible aún ({err}). Reintentando en {retry_interval}s...")
            time.sleep(retry_interval)

    raise ConnectionError(f"No se pudo conectar a RabbitMQ en {host}:{port} tras {max_retries} intentos.")
