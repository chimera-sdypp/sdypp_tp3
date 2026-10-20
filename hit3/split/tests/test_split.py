import io
import json

import fakeredis
import numpy as np
import pytest
from PIL import Image

from split import split

IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"
GRISES = [{"operacion": "grises", "imagen": IMAGEN}]


class CanalQuePublica:
    """Lo que el split usa de un canal de pika: declarar la cola de tareas y publicar."""

    def __init__(self):
        self.colas, self.publicados = set(), []

    def queue_declare(self, queue, durable=False, arguments=None):
        self.colas.add(queue)

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.publicados.append((routing_key, json.loads(body)))


def png(matriz, modo=None):
    salida = io.BytesIO()
    Image.fromarray(matriz, modo).save(salida, format="PNG")
    return salida.getvalue()


def imagen_rgb(alto, ancho, semilla=0):
    return np.random.default_rng(semilla).integers(0, 256, (alto, ancho, 3), dtype=np.uint8)


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


def cargar_job(redis, job_id, archivos, operaciones=GRISES):
    """Deja el job como lo deja el backend (contrato §10) antes de publicarlo en sobel.split."""
    redis.hset(f"job:{job_id}", mapping={
        "usuario": "ana", "estado": "queued", "creado": 1761055391000,
        "operaciones": json.dumps(operaciones), "archivos": len(archivos),
    })
    for a, datos in enumerate(archivos):
        redis.set(f"job:{job_id}:entrada:{a}", datos)


def test_un_job_en_cola_pasa_a_processing(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(8, 5))])

    split.manejador(redis, partes=4)(CanalQuePublica(), b'{"job_id": "j1"}')

    assert redis.hget("job:j1", "estado") == b"processing"


def reconstruir(redis, job_id, archivo):
    """Une los fragmentos del paso 0 recortando los bordes según la geometría (§10), como lo hará el joiner."""
    geometria = {k.decode(): v.decode() for k, v in redis.hgetall(f"job:{job_id}:geometria:{archivo}").items()}
    franjas = []
    for f in range(int(geometria["total"])):
        extra, filas = map(int, geometria[str(f)].split(","))
        fragmento = np.asarray(Image.open(io.BytesIO(redis.get(f"job:{job_id}:frag:{archivo}:{f}:0"))))
        franjas.append(fragmento[extra:extra + filas])
    return np.vstack(franjas)


def test_los_fragmentos_y_la_geometria_reconstruyen_el_archivo(redis):
    original = imagen_rgb(10, 7)
    cargar_job(redis, "j1", [png(original)])

    split.manejador(redis, partes=4)(CanalQuePublica(), b'{"job_id": "j1"}')

    assert redis.hget("job:j1:geometria:0", "total") == b"4"
    np.testing.assert_array_equal(reconstruir(redis, "j1", 0), original)


def test_publica_una_tarea_por_fragmento_a_la_cola_de_la_imagen_del_primer_paso(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(9, 4))])
    canal = CanalQuePublica()

    split.manejador(redis, partes=3, ahora_ms=lambda: 1761055391204)(canal, b'{"job_id": "j1"}')

    cola = "sobel.tareas.ghcr.io/chimera-sdypp/sobel-worker:1.0.0"
    assert canal.colas == {cola}
    assert canal.publicados == [
        (cola, {"job_id": "j1", "archivo": 0, "fragmento": f, "paso": 0, "operaciones": GRISES,
                "intento": 1, "encolado": 1761055391204})
        for f in range(3)
    ]


def test_un_png_con_alfa_se_reparte_en_rgb_sin_el_canal_alfa(redis):
    con_alfa = np.random.default_rng(1).integers(0, 256, (6, 5, 4), dtype=np.uint8)
    cargar_job(redis, "j1", [png(con_alfa, "RGBA")])

    split.manejador(redis, partes=2)(CanalQuePublica(), b'{"job_id": "j1"}')

    np.testing.assert_array_equal(reconstruir(redis, "j1", 0), con_alfa[:, :, :3])
