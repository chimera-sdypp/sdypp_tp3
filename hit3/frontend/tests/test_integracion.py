"""Contra RabbitMQ y Redis de verdad: pytest -m integracion (con RABBITMQ_URL y REDIS_URL)."""

import json
import os
import threading

import pika
import pytest
import redis as redis_py
from fastapi.testclient import TestClient

from comun import colas
from frontend.app import crear_app
from conftest import TOKEN

pytestmark = pytest.mark.integracion
URL_RABBIT = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
URL_REDIS = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture
def redis_real():
    cliente = redis_py.Redis.from_url(URL_REDIS)
    cliente.delete("job:j1")
    yield cliente
    cliente.delete("job:j1")


def test_el_avance_en_redis_llega_al_navegador_hasta_done(backend, redis_real, tmp_path, monkeypatch):
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    app = crear_app({"BACKEND_URL": backend.url, "REDIS_URL": URL_REDIS})
    redis_real.hset("job:j1", mapping={"estado": "processing", "fragmentos_total": 4, "completos": 1})
    eventos = []
    with TestClient(app) as cliente, cliente.stream("GET", "/eventos/j1", headers=TOKEN) as respuesta:
        for linea in respuesta.iter_lines():
            if not linea.startswith("data: "):
                continue
            eventos.append(json.loads(linea[6:]))
            if len(eventos) == 1:
                # El joiner termina el job: el SSE manda el último avance y se cierra solo.
                redis_real.hset("job:j1", mapping={"estado": "done", "completos": 4})
        assert cliente.get("/health").json() == {"frontend": "ok", "redis": "ok"}
    assert [(e["completos"], e["porcentaje"], e["estado"]) for e in eventos] == [(1, 25, "processing"),
                                                                                 (4, 100, "done")]


def test_con_escucha_un_resultado_del_fanout_llega_al_navegador(backend, difusor, redis_real, tmp_path,
                                                                 monkeypatch):
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    app = crear_app({"BACKEND_URL": backend.url, "REDIS_URL": URL_REDIS, "RABBITMQ_URL": URL_RABBIT},
                    difusor=difusor, escuchar=True)
    listo = threading.Event()

    def publicar():
        # Hasta que la cola exclusiva esté atada, el fanout no tiene a quién mandarle: se repite.
        conexion = pika.BlockingConnection(pika.URLParameters(URL_RABBIT))
        canal = conexion.channel()
        colas.declarar(canal)
        while not listo.wait(0.2):
            colas.publicar(canal, "", {"job_id": "j1", "archivo": 0, "fragmento": 3, "worker": "w1", "ms": 5,
                                       "intento": 1}, exchange=colas.RESULTADOS)
        conexion.close()

    threading.Thread(target=publicar, daemon=True).start()
    try:
        with TestClient(app) as cliente, cliente.stream("GET", "/eventos/j1", headers=TOKEN) as respuesta:
            linea = next(l for l in respuesta.iter_lines() if l.startswith("data: "))
            assert json.loads(linea[6:])["fragmento"] == 3
            assert cliente.get("/health").json()["rabbitmq"] == "ok"
    finally:
        listo.set()
