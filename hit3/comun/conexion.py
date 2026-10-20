"""Conexión a RabbitMQ y a Redis con backoff exponencial."""

import logging
import os
import time

import pika
import pika.exceptions
import redis
import redis.exceptions

ESPERA_INICIAL = 1
ESPERA_MAXIMA = 30

ERRORES_DE_CONEXION = (
    pika.exceptions.AMQPConnectionError,
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    OSError,
)
# Subclases de las de arriba, pero reintentar no las arregla: la credencial no cambia mientras viva el pod.
ERRORES_DE_CREDENCIALES = (
    pika.exceptions.ProbableAuthenticationError,
    pika.exceptions.ProbableAccessDeniedError,
    redis.exceptions.AuthenticationError,
)

# Lo que devuelve el /health de cada servicio (contrato §12): {"rabbitmq": "ok", "redis": "caido"}.
estado = {}


def con_backoff(conectar, nombre, dormir=time.sleep, log=logging.getLogger("tp3.comun"),
                intentos_max=None):
    """Llama a `conectar` hasta que ande, esperando 1, 2, 4, 8… s con tope de 30 s.

    Sin `intentos_max` reintenta para siempre; con él, se rinde y propaga el último error.
    """
    espera = ESPERA_INICIAL
    reconexion = 0
    estado[nombre] = "caido"
    while True:
        try:
            resultado = conectar()
            estado[nombre] = "ok"
            log.info("conectado a %s", nombre)
            return resultado
        except ERRORES_DE_CREDENCIALES as error:
            log.error("credenciales inválidas para %s (%s), no reintento", nombre, error)
            raise
        except ERRORES_DE_CONEXION as error:
            if reconexion == intentos_max:
                raise
            reconexion += 1
            log.warning("reconexión %d a %s falló (%r), espero %d s", reconexion, nombre, error, espera)
            dormir(espera)
            espera = min(espera * 2, ESPERA_MAXIMA)


def variable_obligatoria(nombre):
    """Sin default a propósito (contrato §12): si falta el Secret, mejor fallar que reconectar a localhost."""
    try:
        return os.environ[nombre]
    except KeyError:
        raise RuntimeError(f"falta la variable de entorno {nombre}") from None


def conectar_rabbitmq(url=None, log=logging.getLogger("tp3.comun")):
    """Conexión bloqueante a $RABBITMQ_URL, esperando con backoff a que el broker responda."""
    url = url or variable_obligatoria("RABBITMQ_URL")
    parametros = pika.URLParameters(url)
    if "heartbeat=" not in url:
        parametros.heartbeat = 600
    return con_backoff(lambda: pika.BlockingConnection(parametros), "rabbitmq", log=log)


def conectar_redis(url=None, log=logging.getLogger("tp3.comun")):
    """Cliente de $REDIS_URL; el ping explícito es porque redis-py conecta recién en el primer comando."""
    cliente = redis.Redis.from_url(url or variable_obligatoria("REDIS_URL"))
    con_backoff(cliente.ping, "redis", log=log)
    return cliente


def consumir(cola, callback, al_conectar=None, prefetch=1, cliente_redis=None,
             conectar=None, dormir=time.sleep, log=logging.getLogger("tp3.comun")):
    """Consume `cola` llamando a `callback(canal, cuerpo)` por mensaje.

    Si el callback termina bien hace ack; si lanza una excepción, nack sin reencolar (va a la DLQ).
    El canal publica con confirmación del broker, y `al_conectar(canal)` declara la topología.
    Si `al_conectar` devuelve un nombre de cola, se consume esa: sirve para las colas exclusivas que
    RabbitMQ nombra en cada conexión (el frontend, contrato §9). En ese caso `cola` puede ser None.
    Si se cae RabbitMQ, reconecta con backoff; los mensajes sin ack los reencola el broker.
    Si se cae Redis, el mensaje no tiene la culpa: vuelve a la cola y se espera a que Redis responda.
    """
    conectar = conectar or (lambda: conectar_rabbitmq(log=log))

    def manejar(canal, metodo, propiedades, cuerpo):
        try:
            callback(canal, cuerpo)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as error:
            log.warning("se cayó redis procesando un mensaje de %s (%s), lo reencolo", consumida, error)
            canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=True)
            if cliente_redis is not None:
                con_backoff(cliente_redis.ping, "redis", dormir=dormir, log=log)
            return
        except pika.exceptions.AMQPConnectionError:
            raise  # sin canal no hay nack: lo reencola el broker y se reconecta afuera
        except Exception:
            log.exception("falló un mensaje de %s, va a la DLQ", consumida)
            canal.basic_nack(delivery_tag=metodo.delivery_tag, requeue=False)
            return
        canal.basic_ack(delivery_tag=metodo.delivery_tag)

    while True:
        canal = conectar().channel()
        canal.confirm_delivery()
        canal.basic_qos(prefetch_count=prefetch)
        consumida = (al_conectar(canal) if al_conectar else None) or cola
        canal.basic_consume(consumida, on_message_callback=manejar)
        try:
            canal.start_consuming()
            return  # alguien llamó a stop_consuming: apagado ordenado
        except pika.exceptions.AMQPConnectionError as error:
            log.warning("se cortó la conexión con rabbitmq consumiendo %s (%s), reconecto", consumida, error)
