"""Métricas de Prometheus del contrato (§11) que usan los servicios. Se definen una sola vez: un proceso que
registrara dos veces el mismo nombre no arranca, y así ningún servicio se desincroniza en nombres ni buckets."""

import contextlib
import re

import pika.exceptions
import redis.exceptions
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

# Una caída de Redis o de RabbitMQ no es culpa del mensaje: `consumir` lo reencola, no va a la DLQ.
REENCOLAN = (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, pika.exceptions.AMQPConnectionError)

# Los buckets por defecto llegan a 10 s: una tarea sobre un fragmento de 100 MP o un job de minutos quedarían en +Inf.
BUCKETS_TAREA = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60)
BUCKETS_JOB = (0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600)

TAREAS = Counter("sobel_tareas_procesadas_total", "Unidades de trabajo de cada servicio (en las métricas, una tarea "
                 "es cualquier unidad de trabajo, no sólo la del worker)", ["servicio", "operacion", "resultado"])
TIEMPO_TAREA = Histogram("sobel_tiempo_tarea_segundos", "Lo que tardó cada unidad de trabajo",
                         ["servicio", "operacion"], buckets=BUCKETS_TAREA)
ERRORES = Counter("sobel_errores_total", "Errores por tipo", ["servicio", "tipo"])
TIEMPO_JOB = Histogram("sobel_tiempo_job_segundos", "De creado a terminado, de cada job que llega a done",
                       buckets=BUCKETS_JOB)


def exponer():
    """(content type, cuerpo) de GET /metrics."""
    return CONTENT_TYPE_LATEST, generate_latest()


def tipo_de_error(error):
    """El `tipo` de `sobel_errores_total`: `TiempoAgotado` → `TIEMPO_AGOTADO`, `JSONDecodeError` →
    `JSON_DECODE_ERROR`."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", type(error).__name__).upper()


def trabajo(servicio, operacion, segundos):
    """Una unidad de trabajo terminada bien, con lo que tardó."""
    TAREAS.labels(servicio, operacion, "ok").inc()
    TIEMPO_TAREA.labels(servicio, operacion).observe(segundos)


def fallo(servicio, operacion):
    """Una unidad de trabajo que terminó mal."""
    TAREAS.labels(servicio, operacion, "error").inc()


@contextlib.contextmanager
def fallos(servicio, operacion):
    """Cuenta `resultado="error"` si el bloque lanza una excepción que va a la DLQ, y la deja seguir."""
    try:
        yield
    except REENCOLAN:
        raise
    except Exception:
        fallo(servicio, operacion)
        raise
