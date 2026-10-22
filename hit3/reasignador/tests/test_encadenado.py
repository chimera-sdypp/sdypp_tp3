"""El caso de falla de punta a punta (M7), sin RabbitMQ: split → worker que falla → sobel.dlq → reasignador → worker
→ joiner, con fakeredis (seams S4 + S6 + S10 + S9 encadenadas)."""

import io
import json
from types import SimpleNamespace

import fakeredis
import numpy as np
import pytest
from PIL import Image

from joiner import joiner
from reasignador import reasignador
from split import split
from worker import worker
from worker.operaciones import OPERACIONES

IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"
CREADO = 1761055391000


def png(matriz):
    salida = io.BytesIO()
    Image.fromarray(matriz).save(salida, format="PNG")
    return salida.getvalue()


def leer(datos):
    return np.asarray(Image.open(io.BytesIO(datos)))


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


class Broker:
    """Lo que hace RabbitMQ en el encadenado: cada mensaje publicado va, en orden, al servicio que consume su cola.
    Lo que el worker rechaza (una excepción: `consumir` hace nack) va a sobel.dlq con los headers de muerte que pone
    RabbitMQ, y lo que llega a sobel.dlq.agotados queda guardado."""

    def __init__(self):
        self.pendientes, self.agotados = [], []

    def queue_declare(self, queue, durable=False, arguments=None):
        pass

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.pendientes.append((exchange, routing_key, body))

    def correr(self, trabajador, uniones, reasignar):
        while self.pendientes:
            exchange, cola, cuerpo = self.pendientes.pop(0)
            if exchange == "sobel.resultados":
                uniones(self, cuerpo)
            elif cola == "sobel.dlq.agotados":
                self.agotados.append(json.loads(cuerpo))
            elif cola == f"sobel.tareas.{IMAGEN}":
                try:
                    trabajador(self, cuerpo)
                except Exception:
                    reasignar(self, cuerpo, SimpleNamespace(headers={
                        "x-first-death-queue": cola, "x-first-death-reason": "rejected",
                        "x-death": [{"queue": cola, "reason": "rejected", "count": 1, "exchange": ""}]}))
            else:
                raise AssertionError(f"nadie consume {exchange!r}/{cola!r}")


def cargar_job(redis, archivos, operaciones):
    """Deja el job como lo deja el backend (contrato §10) antes de publicarlo en sobel.split."""
    redis.hset("job:j1", mapping={
        "usuario": "ana", "estado": "queued", "creado": CREADO, "operaciones": json.dumps(operaciones),
        "archivos": len(archivos), "fragmentos_total": 0, "completos": 0, "en_dlq": 0, "reintentos": 0})
    for a, datos in enumerate(archivos):
        redis.set(f"job:j1:entrada:{a}", datos)


def procesar_job(redis, trabajador):
    broker = Broker()
    split.manejador(redis, partes=3)(broker, json.dumps({"job_id": "j1"}).encode())
    broker.correr(trabajador, joiner.manejador(redis), reasignador.manejador(redis, reintentos_max=3))
    return broker


def aplicar_entera(original, operaciones):
    """La cadena aplicada a la imagen entera, sin partir: la referencia del procesamiento distribuido.

    Usa las operaciones del worker a propósito: mide que fallar, reintentar, partir y unir no cambie nada. Que cada
    operación esté bien lo prueban los tests del worker.
    """
    for paso in operaciones:
        original = OPERACIONES[paso["operacion"]](original)
    return original


def falla_la_primera_vez(trabajador, archivo, fragmento, paso):
    """El worker falla (nack) la primera vez que le llega esa tarea; las siguientes las procesa bien. Que muera con
    la tarea en la mano lo prueba la prueba de humo, contra RabbitMQ."""
    fallo = False

    def procesar(canal, cuerpo):
        nonlocal fallo
        tarea = json.loads(cuerpo)
        if (tarea["archivo"], tarea["fragmento"], tarea["paso"]) == (archivo, fragmento, paso) and not fallo:
            fallo = True
            raise RuntimeError("falló el worker con esta tarea")
        trabajador(canal, cuerpo)

    return procesar


OPERACIONES_DEL_JOB = [{"operacion": "grises", "imagen": IMAGEN}, {"operacion": "sobel", "imagen": IMAGEN}]


def test_si_un_worker_falla_con_una_tarea_se_reasigna_y_el_job_termina_igual(redis):
    generador = np.random.default_rng(11)
    originales = [generador.integers(0, 256, (alto, 7, 3), dtype=np.uint8) for alto in (11, 5)]
    cargar_job(redis, [png(o) for o in originales], OPERACIONES_DEL_JOB)
    # Falla el paso de sobel (el segundo) del fragmento del medio del primer archivo.
    trabajador = falla_la_primera_vez(worker.manejador(redis, nombre="w1"), archivo=0, fragmento=1, paso=1)

    procesar_job(redis, trabajador)

    assert redis.hmget("job:j1", "estado", "reintentos", "en_dlq") == [b"done", b"1", b"0"]
    assert redis.hget("job:j1", "completos") == redis.hget("job:j1", "fragmentos_total") == b"6"
    for archivo, original in enumerate(originales):
        np.testing.assert_array_equal(leer(redis.get(f"job:j1:resultado:{archivo}")),
                                      aplicar_entera(original, OPERACIONES_DEL_JOB))


def test_si_ningun_worker_ofrece_la_operacion_las_tareas_se_agotan_y_el_job_queda_en_dlq(redis):
    original = np.random.default_rng(12).integers(0, 256, (6, 7, 3), dtype=np.uint8)
    cargar_job(redis, [png(original)], OPERACIONES_DEL_JOB)
    trabajador = worker.manejador(redis, nombre="w1", ofrecidas=("grises",))  # no ofrece sobel

    broker = procesar_job(redis, trabajador)

    # 6 filas en 3 partes son 3 fragmentos; cada tarea de sobel se intenta 4 veces (3 reintentos) y se agota.
    assert redis.hmget("job:j1", "estado", "en_dlq", "reintentos", "completos") == [b"dlq", b"3", b"9", b"0"]
    assert sorted((t["fragmento"], t["paso"], t["intento"]) for t in broker.agotados) == [(0, 1, 4), (1, 1, 4),
                                                                                          (2, 1, 4)]
    assert json.loads(redis.hget("job:j1", "error"))["tipo"] == "REINTENTOS_AGOTADOS"
    assert redis.llen("terminados") == 1 and redis.lindex("terminados", 0).startswith(b"dlq ")
