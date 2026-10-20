import io
import json
import os

import fakeredis
import pytest
from fastapi.testclient import TestClient
from PIL import Image

from backend import auth
from backend.app import crear_app
from backend.config import Config
from backend.rabbit import NoDisponible

SECRETO = "s" * 32
IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"
OTRA = "ghcr.io/chimera-sdypp/sobel-worker:1.1.0"


class RabbitFalso:
    """Lo que el backend usa de RabbitMQ, en memoria: {cola: [mensajes]}."""

    def __init__(self):
        self.colas = {}
        self.caido = False

    def publicar(self, cola, mensaje):
        self._revisar()
        self.colas.setdefault(cola, []).append(mensaje)

    def contar(self, imagenes):
        self._revisar()
        nombres = ["sobel.split", "sobel.joiner", "sobel.dlq", "sobel.dlq.agotados"]
        nombres += [f"sobel.tareas.{i}" for i in sorted(imagenes)]
        return {cola: len(self.colas.get(cola, [])) for cola in nombres}

    def reprocesar_agotados(self):
        self._revisar()
        movidos = []
        for tarea in self.colas.pop("sobel.dlq.agotados", []):
            tarea.update(intento=1)
            self.publicar(f"sobel.tareas.{tarea['operaciones'][tarea['paso']]['imagen']}", tarea)
            movidos.append(tarea["job_id"])
        return movidos

    def vivo(self):
        return not self.caido

    def _revisar(self):
        if self.caido:
            raise NoDisponible()


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


@pytest.fixture
def rabbit():
    return RabbitFalso()


@pytest.fixture
def config():
    return Config(secreto=SECRETO, imagenes_permitidas=frozenset({IMAGEN, OTRA}), pedidos_por_minuto=1000)


@pytest.fixture
def cliente(config, redis, rabbit, tmp_path, monkeypatch):
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    app = crear_app(config, redis, rabbit, mantener=False)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def worker(redis):
    """Registra un worker de IMAGEN que ofrece las tres operaciones."""
    redis.hset("worker:w1", mapping={"imagen": IMAGEN, "operaciones": "sobel,grises,blur", "host": "w1"})
    return "w1"


def token(usuario="ana", rol="usuario", horas=1):
    return {"Authorization": f"Bearer {auth.emitir(usuario, rol, SECRETO, horas)}"}


def png(ancho=8, alto=6, color=(10, 200, 30)):
    salida = io.BytesIO()
    Image.new("RGB", (ancho, alto), color).save(salida, "PNG")
    return salida.getvalue()


def ruido(ancho, alto):
    """PNG que no se comprime: pesa unos 3 bytes por píxel."""
    salida = io.BytesIO()
    Image.frombytes("RGB", (ancho, alto), os.urandom(ancho * alto * 3)).save(salida, "PNG")
    return salida.getvalue()


def pasos(*operaciones, imagen=IMAGEN):
    return json.dumps([{"operacion": o, "imagen": imagen} for o in operaciones])


def enviar(cliente, archivos=None, operaciones=None, cabeceras=None, **extra):
    archivos = [("archivos", (f"a{i}.png", datos, "image/png")) for i, datos in enumerate(archivos or [png()])]
    datos = {"operaciones": operaciones if operaciones is not None else pasos("sobel"), **extra}
    return cliente.post("/jobs", files=archivos, data=datos, headers=cabeceras or token())
