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

QUORUM = {"x-queue-type": "quorum"}


def declarar(canal):
    """Declara la topología fija. Es idempotente: cada servicio la llama al conectarse."""
    canal.exchange_declare(DLX, exchange_type="direct", durable=True)
    canal.exchange_declare(RESULTADOS, exchange_type="fanout", durable=True)
    for cola in (SPLIT, DLQ, AGOTADOS, JOINER):
        canal.queue_declare(cola, durable=True, arguments=QUORUM)
    canal.queue_bind(DLQ, DLX, routing_key=DLQ)
    canal.queue_bind(JOINER, RESULTADOS, routing_key="")


def declarar_tareas(canal, imagen):
    """Declara la cola de tareas de una imagen de worker y devuelve su nombre.

    La routing key fija de dead-letter hace que una sola atadura de la DLQ sirva para todas las
    imágenes; sin ella, el mensaje conserva la key de su cola y el exchange direct lo tira.
    """
    cola = f"sobel.tareas.{imagen}"
    canal.queue_declare(cola, durable=True, arguments={
        **QUORUM,
        "x-dead-letter-exchange": DLX,
        "x-dead-letter-routing-key": DLQ,
        # Reentregas después de que un worker muere con el mensaje: con 3, la 4.ª caída lo manda a la DLQ.
        "x-delivery-limit": int(os.environ.get("SOBEL_LIMITE_ENTREGAS", "3")),
    })
    return cola


def publicar(canal, cola, mensaje):
    """Publica `mensaje` como JSON persistente (§9). Con el canal en modo confirm, vuelve cuando el broker lo aceptó."""
    canal.basic_publish("", cola, json.dumps(mensaje).encode(), properties=pika.BasicProperties(
        content_type="application/json", delivery_mode=pika.DeliveryMode.Persistent))
