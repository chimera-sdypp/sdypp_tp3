"""Contra RabbitMQ y Redis de verdad: pytest -m integracion (con RABBITMQ_URL y REDIS_URL)."""

import json
import os
import uuid

import pika
import pytest
import redis as redis_py
from fastapi.testclient import TestClient

from backend.app import crear_app
from backend.rabbit import Rabbit
from comun import colas
from conftest import IMAGEN, enviar, pasos, png, token

pytestmark = pytest.mark.integracion
URL_RABBIT = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
URL_REDIS = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture
def canal():
    conexion = pika.BlockingConnection(pika.URLParameters(URL_RABBIT))
    canal = conexion.channel()
    canal.confirm_delivery()  # que basic_publish espere al broker: si no, el basic_get de atrás puede llegar antes
    colas.declarar(canal)
    for cola in (colas.SPLIT, colas.DLQ, colas.AGOTADOS, colas.JOINER):
        canal.queue_purge(cola)
    canal.queue_purge(colas.declarar_tareas(canal, IMAGEN))
    yield canal
    conexion.close()


@pytest.fixture
def redis():
    cliente = redis_py.Redis.from_url(URL_REDIS)
    cliente.flushdb()
    return cliente


def sacar(canal, cola):
    metodo, _, cuerpo = canal.basic_get(cola, auto_ack=True)
    return json.loads(cuerpo) if metodo else None


def test_post_encola_en_sobel_split(canal, redis, config, tmp_path, monkeypatch):
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    redis.hset("worker:w1", mapping={"imagen": IMAGEN, "operaciones": "sobel"})
    cliente = TestClient(crear_app(config, redis, Rabbit(URL_RABBIT), mantener=False))
    respuesta = enviar(cliente, [png()], pasos("sobel"))
    assert respuesta.status_code == 202
    assert sacar(canal, colas.SPLIT) == {"job_id": respuesta.json()["contenido"]["job_id"]}
    assert cliente.get("/health").json()["contenido"]["rabbitmq"] == "ok"
    stats = cliente.get("/stats", headers=token("root", "admin")).json()["contenido"]
    assert stats["colas"][f"sobel.tareas.{IMAGEN}"] == 0 and stats["workers_activos"] == 1


def test_reprocesar_agotados(canal):
    job_id = str(uuid.uuid4())
    tarea = {"job_id": job_id, "archivo": 0, "fragmento": 0, "paso": 0, "intento": 4,
             "operaciones": [{"operacion": "sobel", "imagen": IMAGEN}], "encolado": 0}
    canal.basic_publish("", colas.AGOTADOS, json.dumps(tarea).encode())
    canal.basic_publish("", colas.AGOTADOS, b"no es json")
    assert Rabbit(URL_RABBIT).reprocesar_agotados() == [job_id]
    movida = sacar(canal, f"sobel.tareas.{IMAGEN}")
    assert movida["intento"] == 1 and movida["encolado"] > 0
    assert sacar(canal, colas.AGOTADOS) is None


def test_sin_broker_falla_rapido_y_espera_para_reintentar():
    reloj = [0.0]
    rabbit = Rabbit("amqp://guest:guest@127.0.0.1:1/%2F", reloj=lambda: reloj[0])
    assert not rabbit.vivo()
    assert rabbit.proximo_intento == 1  # 1 s de espera antes del próximo intento
    assert not rabbit.vivo() and rabbit.espera == 2  # todavía no pasó: ni lo intenta
    reloj[0] = 1.5
    assert not rabbit.vivo() and rabbit.espera == 4
