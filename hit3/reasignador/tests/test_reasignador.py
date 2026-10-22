import json
from types import SimpleNamespace

import fakeredis
import pika.exceptions
import pytest

from reasignador import reasignador

GRISES = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"
SOBEL = "ghcr.io/chimera-sdypp/sobel-worker:2.0.0"
CREADO = 1761055391000
AHORA = 1761055399000


class Canal:
    """Lo que el reasignador usa de un canal de pika: declarar colas y publicar, en orden."""

    def __init__(self):
        self.eventos = []

    def queue_declare(self, queue, durable=False, arguments=None):
        self.eventos.append(("declarar", queue))

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.eventos.append(("publicar", routing_key, json.loads(body)))


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


OPERACIONES = [{"operacion": "grises", "imagen": GRISES}, {"operacion": "sobel", "imagen": SOBEL}]


def preparar_job(redis, estado="processing"):
    redis.hset("job:j1", mapping={"usuario": "ana", "estado": estado, "creado": CREADO, "archivos": 1,
                                  "operaciones": json.dumps(OPERACIONES), "fragmentos_total": 4, "completos": 0,
                                  "en_dlq": 0, "reintentos": 0})
    redis.expire("job:j1", 86400)


def tarea(paso=1, intento=1):
    """Una tarea como la publica el worker (contrato §9): el paso 1 es sobel, en la imagen SOBEL."""
    return {"job_id": "j1", "archivo": 0, "fragmento": 3, "paso": paso, "operaciones": OPERACIONES,
            "intento": intento, "encolado": 1761055392000}


def muerta(mensaje, cola=f"sobel.tareas.{SOBEL}", motivo="rejected"):
    """El mensaje como llega a sobel.dlq: el cuerpo y las propiedades con los headers que pone RabbitMQ."""
    cuerpo = mensaje if isinstance(mensaje, bytes) else json.dumps(mensaje).encode()
    headers = {"x-first-death-queue": cola, "x-first-death-reason": motivo,
               "x-death": [{"queue": cola, "reason": motivo, "count": 1, "exchange": ""}]}
    return cuerpo, SimpleNamespace(headers=headers)


def manejador(redis, **opciones):
    return reasignador.manejador(redis, **{"reintentos_max": 3, "ahora_ms": lambda: AHORA, **opciones})


def test_una_tarea_que_fallo_vuelve_a_la_cola_de_la_imagen_de_su_paso_con_un_intento_mas(redis, caplog):
    preparar_job(redis)
    canal = Canal()

    with caplog.at_level("INFO"):
        manejador(redis)(canal, *muerta(tarea(paso=1, intento=1), motivo="delivery_limit"))

    cola = f"sobel.tareas.{SOBEL}"
    assert canal.eventos == [("declarar", cola), ("publicar", cola, {**tarea(paso=1), "intento": 2, "encolado": AHORA})]
    assert redis.hget("job:j1", "reintentos") == b"1"
    assert "j1: archivo 0, fragmento 3, paso 1 (delivery_limit): intento 2" in caplog.text


def test_con_el_intento_en_el_maximo_de_reintentos_todavia_se_reintenta(redis):
    preparar_job(redis)
    canal = Canal()

    manejador(redis, reintentos_max=3)(canal, *muerta(tarea(intento=3)))

    assert canal.eventos[-1] == ("publicar", f"sobel.tareas.{SOBEL}", {**tarea(), "intento": 4, "encolado": AHORA})


def test_la_tarea_de_un_job_vencido_se_descarta_con_un_aviso_y_sin_recrear_el_job(redis, caplog):
    canal = Canal()

    manejador(redis)(canal, *muerta(tarea()))

    assert canal.eventos == [] and redis.keys() == []
    assert [r.levelname for r in caplog.records] == ["WARNING"] and "j1" in caplog.text


def sin(campo):
    return {k: v for k, v in tarea().items() if k != campo}


@pytest.mark.parametrize("cuerpo, headers", [
    (b"no es json", None),
    (json.dumps(sin("job_id")).encode(), None),
    (json.dumps(sin("paso")).encode(), None),
    (json.dumps(sin("operaciones")).encode(), None),
    (json.dumps(sin("intento")).encode(), None),
    (json.dumps(sin("fragmento")).encode(), None),
    (json.dumps({**tarea(), "paso": 7}).encode(), None),  # no hay paso 7 en la cadena
    (json.dumps({**tarea(), "paso": -1}).encode(), None),  # en Python, el último
    (json.dumps({**tarea(), "paso": True}).encode(), None),  # en Python, el 1
    (json.dumps({**tarea(), "intento": "1"}).encode(), None),
    (json.dumps({**tarea(), "intento": 0}).encode(), None),
    (b'"un job_id"', {"x-first-death-queue": "sobel.split"}),  # JSON, pero no un objeto
    (json.dumps(tarea()).encode(), {}),  # sin x-first-death-queue
    (json.dumps(tarea()).encode(), {"x-first-death-queue": "sobel.dlq.agotados"}),  # una cola sin DLX
], ids=["no_json", "sin_job_id", "sin_paso", "sin_operaciones", "sin_intento", "sin_fragmento", "paso_inexistente",
        "paso_negativo", "paso_booleano", "intento_texto", "intento_cero", "no_es_objeto", "sin_header",
        "cola_desconocida"])
def test_un_mensaje_ilegible_se_descarta_con_un_error_en_el_log(redis, caplog, cuerpo, headers):
    preparar_job(redis)
    antes = redis.hgetall("job:j1")
    canal = Canal()
    propiedades = muerta(tarea())[1] if headers is None else SimpleNamespace(headers=headers)

    manejador(redis)(canal, cuerpo, propiedades)

    assert canal.eventos == [] and redis.hgetall("job:j1") == antes
    assert [r.levelname for r in caplog.records] == ["ERROR"]


class CanalQueSeCae(Canal):
    def basic_publish(self, exchange, routing_key, body, properties=None):
        raise pika.exceptions.StreamLostError("se cortó la conexión al publicar")


def test_si_se_cae_rabbitmq_al_publicar_el_reintento_no_lo_cuenta(redis):
    preparar_job(redis)

    # consumir no hace ack y reconecta: RabbitMQ vuelve a entregar el mensaje de la DLQ.
    with pytest.raises(pika.exceptions.StreamLostError):
        manejador(redis)(CanalQueSeCae(), *muerta(tarea()))

    assert redis.hget("job:j1", "reintentos") == b"0"


class VenceAntesDeSumar:
    """El Redis de verdad, salvo que el job vence justo después de publicar el reintento."""

    def __init__(self, redis):
        self.redis = redis

    def __getattr__(self, nombre):
        return getattr(self.redis, nombre)

    def transaction(self, *args, **opciones):
        self.redis.delete("job:j1")
        return self.redis.transaction(*args, **opciones)


def test_si_el_job_vence_justo_despues_de_publicar_no_lo_recrea(redis):
    preparar_job(redis)

    manejador(VenceAntesDeSumar(redis))(Canal(), *muerta(tarea()))

    assert redis.keys() == []
