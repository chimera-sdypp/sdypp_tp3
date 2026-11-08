"""Contra el emulador de GCS (fake-gcs-server): pytest -m integracion con SOBEL_ALMACEN_URL.

docker run -d -p 4443:4443 fsouza/fake-gcs-server:1.56.1 -scheme http -port 4443
SOBEL_ALMACEN_URL=http://localhost:4443 python -m pytest -m integracion comun
"""

import os
import uuid

import fakeredis
import pytest

from comun import almacen
from comun.almacen import Almacen

pytestmark = pytest.mark.integracion
URL = os.environ.get("SOBEL_ALMACEN_URL")


@pytest.fixture
def bucket():
    if not URL:
        pytest.skip("sin SOBEL_ALMACEN_URL")
    cliente = Almacen(f"prueba-{uuid.uuid4().hex[:8]}", url=URL)
    cliente.asegurar_bucket()
    cliente.asegurar_bucket()  # dos veces: la segunda es un 409 y no falla
    return cliente


def test_guardar_leer_y_borrar_un_objeto_de_verdad(bucket):
    datos = os.urandom(3 * 1024 * 1024)  # 3 MB: más que un fragmento típico
    bucket.guardar("job/j1/frag/0/2/1.png", datos)
    assert bucket.leer("job/j1/frag/0/2/1.png") == datos
    bucket.borrar(["job/j1/frag/0/2/1.png", "job/j1/frag/0/2/9.png"])  # el segundo no existe
    assert bucket.leer("job/j1/frag/0/2/1.png") is None


def test_el_puntero_en_redis_y_los_bytes_en_el_bucket(bucket):
    redis = fakeredis.FakeRedis()
    pares = [(f"job:j1:frag:0:{f}:0", os.urandom(1000 + f)) for f in range(12)]
    almacen.escribir_varios(redis, bucket, pares, ttl=60)

    assert redis.get("job:j1:frag:0:3:0") == b"job/j1/frag/0/3/0.png"
    assert almacen.leer_varios(redis, bucket, [c for c, _ in pares]) == [d for _, d in pares]
    almacen.borrar(redis, bucket, *[c for c, _ in pares])
    assert redis.keys("*") == [] and bucket.leer("job/j1/frag/0/3/0.png") is None


def test_vivo_contra_el_emulador(bucket):
    assert bucket.vivo()
    assert not Almacen("no-existe-este-bucket", url=URL).vivo()
