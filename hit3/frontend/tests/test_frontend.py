import asyncio
import json
import threading
import time

import pytest
import redis as redis_py

from comun import conexion
from frontend.app import Difusor, crear_app
from conftest import TOKEN


def publicar_cuando_mire(difusor, resultados, job_id="j1"):
    """Espera a que un navegador se suscriba a `job_id` y le manda `resultados` desde otro hilo, como pika."""
    def enviar():
        for _ in range(200):
            if job_id in difusor.suscriptos:
                break
            time.sleep(0.01)
        for resultado in resultados:
            difusor.publicar(resultado)
    threading.Thread(target=enviar, daemon=True).start()


def leer_eventos(respuesta, cantidad):
    eventos = []
    for linea in respuesta.iter_lines():
        if linea.startswith("data: "):
            eventos.append(json.loads(linea[6:]))
            if len(eventos) == cantidad:
                return eventos


def test_la_pagina_y_sus_archivos(cliente):
    respuesta = cliente.get("/")
    assert respuesta.status_code == 200 and "Sobel distribuido" in respuesta.text
    assert "default-src 'self'" in respuesta.headers["content-security-policy"]
    assert cliente.get("/estatico/app.js").status_code == 200
    assert cliente.get("/estatico/estilo.css").status_code == 200


def test_eventos_manda_solo_los_resultados_de_ese_job(cliente, difusor):
    publicar_cuando_mire(difusor, [{"job_id": "otro", "fragmento": 9},
                                   {"job_id": "j1", "archivo": 0, "fragmento": 1, "worker": "w1"},
                                   {"job_id": "j1", "archivo": 0, "fragmento": 2, "worker": "w2"}])
    with cliente.stream("GET", "/eventos/j1", headers=TOKEN) as respuesta:
        assert respuesta.status_code == 200
        assert respuesta.headers["content-type"].startswith("text/event-stream")
        eventos = leer_eventos(respuesta, 2)
    assert [e["fragmento"] for e in eventos] == [1, 2]


def test_al_cerrar_el_navegador_se_desuscribe(cliente, difusor):
    publicar_cuando_mire(difusor, [{"job_id": "j1", "fragmento": 0}])
    with cliente.stream("GET", "/eventos/j1", headers=TOKEN) as respuesta:
        leer_eventos(respuesta, 1)
    for _ in range(100):
        if not difusor.suscriptos:
            break
        time.sleep(0.01)
    assert difusor.suscriptos == {}


@pytest.mark.parametrize("cabeceras, ruta, codigo", [
    ({}, "/eventos/j1", 401),
    ({"Authorization": "Bearer de-otro"}, "/eventos/j1", 404),
    (TOKEN, "/eventos/j2", 404),
])
def test_eventos_exige_poder_ver_el_job(cliente, difusor, cabeceras, ruta, codigo):
    respuesta = cliente.get(ruta, headers=cabeceras)
    assert respuesta.status_code == codigo
    assert respuesta.json()["codigo"] == codigo  # el error del backend llega tal cual
    assert difusor.suscriptos == {}


def test_el_job_id_no_puede_salirse_de_jobs(cliente, backend):
    cliente.get("/eventos/x%3Fusuario=ana", headers=TOKEN)
    assert backend.pedidos == [("/jobs/x%3Fusuario%3Dana", "Bearer bueno")]


def test_sin_backend_es_503(redis, difusor, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    app = crear_app({"BACKEND_URL": "http://127.0.0.1:1", "REDIS_URL": "redis://x"}, redis, difusor, escuchar=False)
    respuesta = TestClient(app).get("/eventos/j1", headers=TOKEN)
    assert respuesta.status_code == 503
    assert respuesta.json()["contenido"]["error"]["tipo"] == "SERVICIO_NO_DISPONIBLE"


def test_ofertas_junta_las_operaciones_de_los_workers_activos(cliente, redis):
    redis.hset("worker:a", mapping={"imagen": "r/w:1", "operaciones": "sobel,grises"})
    redis.hset("worker:b", mapping={"imagen": "r/w:1", "operaciones": "blur"})
    redis.hset("worker:c", mapping={"imagen": "r/w:2", "operaciones": "sobel"})
    redis.set("otra", "cosa")
    assert cliente.get("/ofertas").json()["contenido"]["ofertas"] == {
        "r/w:1": ["blur", "grises", "sobel"], "r/w:2": ["sobel"]}


def test_ofertas_sin_redis_es_503(cliente, redis, monkeypatch):
    def caido(*args, **kwargs):
        raise redis_py.exceptions.ConnectionError("sin redis")
    monkeypatch.setattr(redis, "scan_iter", caido)
    respuesta = cliente.get("/ofertas")
    assert respuesta.status_code == 503
    assert respuesta.headers["Retry-After"] == "5"
    assert respuesta.json()["contenido"]["error"]["tipo"] == "SERVICIO_NO_DISPONIBLE"


def test_health(cliente, monkeypatch):
    monkeypatch.setattr(conexion, "estado", {})
    respuesta = cliente.get("/health")
    assert respuesta.status_code == 503
    assert respuesta.json() == {"frontend": "ok", "rabbitmq": "caido", "redis": "ok"}
    monkeypatch.setattr(conexion, "estado", {"rabbitmq": "ok"})
    assert cliente.get("/health").status_code == 200


def test_metrics(cliente):
    assert "sobel_frontend_navegadores" in cliente.get("/metrics").text


@pytest.mark.parametrize("falta", ["BACKEND_URL", "REDIS_URL", "RABBITMQ_URL"])
def test_no_arranca_sin_configuracion(falta, tmp_path, monkeypatch):
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    entorno = {"BACKEND_URL": "http://backend:8080", "REDIS_URL": "redis://x", "RABBITMQ_URL": "amqp://x"}
    with pytest.raises(ValueError, match=falta):
        crear_app(entorno | {falta: ""})


def test_un_navegador_lento_no_frena_a_los_demas():
    async def probar():
        difusor = Difusor()
        lento, _ = difusor.suscribir("j1")
        rapido, _ = difusor.suscribir("j1")
        for n in range(1001):
            difusor.publicar({"job_id": "j1", "fragmento": n})
        await asyncio.sleep(0)
        assert lento.qsize() == 1000  # el que sobra se descarta, no bloquea al hilo de pika
        while not rapido.empty():
            rapido.get_nowait()
        difusor.publicar({"job_id": "j1", "fragmento": "nuevo"})
        await asyncio.sleep(0)
        assert rapido.get_nowait()["fragmento"] == "nuevo"
    asyncio.run(probar())
