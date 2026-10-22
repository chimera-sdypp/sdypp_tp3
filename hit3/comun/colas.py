"""Topología de RabbitMQ del contrato (§9). Es el único lugar donde se declaran colas y exchanges:
si dos servicios declaran la misma cola con argumentos distintos, RabbitMQ les cierra el canal."""

import json
import os

import pika

SPLIT = "sobel.split"
DLX = "sobel.dlx"
DLQ = "sobel.dlq"
AGOTADOS = "sobel.dlq.agotados"
RESULTADOS = "sobel.resultados"
JOINER = "sobel.joiner"

ARGUMENTOS_QUORUM = {"x-queue-type": "quorum"}


def argumentos_con_dlq():
    """Quorum con dead-letter a la DLQ: lo que un consumidor rechaza (nack sin reencolar) o lo que lo tira abajo más
    veces que SOBEL_LIMITE_ENTREGAS (crash, OOM) va a sobel.dlq en vez de reintentarse para siempre (§9).

    La routing key fija hace que una sola atadura de la DLQ sirva para todas las colas; sin ella, el mensaje conserva
    la key de su cola y el exchange direct lo tira.
    """
    return {
        **ARGUMENTOS_QUORUM,
        "x-dead-letter-exchange": DLX,
        "x-dead-letter-routing-key": DLQ,
        # Reentregas después de que el consumidor muere con el mensaje: con 3, la 4.ª caída lo manda a la DLQ.
        "x-delivery-limit": int(os.environ.get("SOBEL_LIMITE_ENTREGAS", "3")),
    }


def declarar(canal):
    """Declara la topología fija. Es idempotente: cada servicio la llama al conectarse."""
    canal.exchange_declare(DLX, exchange_type="direct", durable=True)
    canal.exchange_declare(RESULTADOS, exchange_type="fanout", durable=True)
    for cola in (SPLIT, JOINER):
        canal.queue_declare(cola, durable=True, arguments=argumentos_con_dlq())
    for cola in (DLQ, AGOTADOS):
        canal.queue_declare(cola, durable=True, arguments=ARGUMENTOS_QUORUM)
    canal.queue_bind(DLQ, DLX, routing_key=DLQ)
    canal.queue_bind(JOINER, RESULTADOS, routing_key="")


def declarar_tareas(canal, imagen_worker):
    """Declara la cola de tareas de una imagen de worker, con dead-letter a la DLQ, y devuelve su nombre."""
    cola = f"sobel.tareas.{imagen_worker}"
    canal.queue_declare(cola, durable=True, arguments=argumentos_con_dlq())
    return cola


def declarar_exclusiva(canal):
    """Declara la cola del frontend: exclusiva, con nombre de RabbitMQ y atada a sobel.resultados.

    Hay una por réplica y se borra sola al cortarse la conexión. Devuelve el nombre, para `consumir`.
    """
    cola = canal.queue_declare("", exclusive=True, auto_delete=True).method.queue
    canal.queue_bind(cola, RESULTADOS, routing_key="")
    return cola


def publicar(canal, cola, mensaje, *, exchange=""):
    """Publica `mensaje` como JSON persistente (§9); con confirms, vuelve cuando el broker lo aceptó.

    Sin `exchange` va directo a `cola`. Al fanout: `publicar(canal, "", resultado, exchange=RESULTADOS)`.
    """
    canal.basic_publish(exchange, cola, json.dumps(mensaje).encode(), properties=pika.BasicProperties(
        content_type="application/json", delivery_mode=pika.DeliveryMode.Persistent))
