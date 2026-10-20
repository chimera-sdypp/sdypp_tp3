import io
import json

import fakeredis
import numpy as np
import pytest
from PIL import Image

from split import split

IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"


def cadena(*operaciones):
    return [{"operacion": op, "imagen": IMAGEN} for op in operaciones]


class CanalQuePublica:
    """Lo que el split usa de un canal de pika: declarar la cola de tareas y publicar, en orden."""

    def __init__(self):
        self.eventos = []

    def queue_declare(self, queue, durable=False, arguments=None):
        self.eventos.append(("declarar", queue))

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.eventos.append(("publicar", routing_key, json.loads(body)))


def png(matriz, modo=None):
    salida = io.BytesIO()
    Image.fromarray(matriz, modo).save(salida, format="PNG")
    return salida.getvalue()


def imagen_rgb(alto, ancho, semilla=0):
    return np.random.default_rng(semilla).integers(0, 256, (alto, ancho, 3), dtype=np.uint8)


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


def cargar_job(redis, job_id, archivos, operaciones=cadena("grises")):
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


def leer_fragmento(redis, job_id, archivo, fragmento):
    return np.asarray(Image.open(io.BytesIO(redis.get(f"job:{job_id}:frag:{archivo}:{fragmento}:0"))))


def reconstruir(redis, job_id, archivo):
    """Une los fragmentos del paso 0 recortando los bordes según la geometría (§10), como lo hará el joiner."""
    geometria = {k.decode(): v.decode() for k, v in redis.hgetall(f"job:{job_id}:geometria:{archivo}").items()}
    franjas = []
    for f in range(int(geometria["total"])):
        extra, filas = map(int, geometria[str(f)].split(","))
        franjas.append(leer_fragmento(redis, job_id, archivo, f)[extra:extra + filas])
    return np.vstack(franjas)


def test_los_fragmentos_y_la_geometria_reconstruyen_el_archivo(redis):
    original = imagen_rgb(10, 7)
    cargar_job(redis, "j1", [png(original)])

    split.manejador(redis, partes=4)(CanalQuePublica(), b'{"job_id": "j1"}')

    assert redis.hget("job:j1:geometria:0", "total") == b"4"
    np.testing.assert_array_equal(reconstruir(redis, "j1", 0), original)


def test_declara_la_cola_de_la_imagen_del_primer_paso_y_publica_una_tarea_por_fragmento(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(9, 4))])
    canal = CanalQuePublica()

    split.manejador(redis, partes=3, ahora_ms=lambda: 1761055391204)(canal, b'{"job_id": "j1"}')

    cola = "sobel.tareas.ghcr.io/chimera-sdypp/sobel-worker:1.0.0"
    assert canal.eventos == [("declarar", cola)] + [
        ("publicar", cola, {"job_id": "j1", "archivo": 0, "fragmento": f, "paso": 0, "operaciones": cadena("grises"),
                            "intento": 1, "encolado": 1761055391204})
        for f in range(3)
    ]


def test_un_png_con_alfa_se_reparte_en_rgb_sin_el_canal_alfa(redis):
    con_alfa = np.random.default_rng(1).integers(0, 256, (6, 5, 4), dtype=np.uint8)
    cargar_job(redis, "j1", [png(con_alfa, "RGBA")])

    split.manejador(redis, partes=2)(CanalQuePublica(), b'{"job_id": "j1"}')

    np.testing.assert_array_equal(reconstruir(redis, "j1", 0), con_alfa[:, :, :3])


# 10 filas en 4 partes: límites 0, 2, 5, 7, 10. Cada operación de vecindad suma una fila de borde por lado,
# salvo donde la imagen se termina (arriba del primer fragmento y abajo del último).
@pytest.mark.parametrize("operaciones, geometria, altos", [
    (cadena("grises"), {"0": "0,2", "1": "0,3", "2": "0,2", "3": "0,3"}, [2, 3, 2, 3]),
    (cadena("sobel"), {"0": "0,2", "1": "1,3", "2": "1,2", "3": "1,3"}, [3, 5, 4, 4]),
    (cadena("blur", "sobel"), {"0": "0,2", "1": "2,3", "2": "2,2", "3": "2,3"}, [4, 7, 6, 5]),
    (cadena("grises", "blur"), {"0": "0,2", "1": "1,3", "2": "1,2", "3": "1,3"}, [3, 5, 4, 4]),
])
def test_cada_fragmento_lleva_una_fila_de_borde_por_operacion_de_vecindad(redis, operaciones, geometria, altos):
    original = imagen_rgb(10, 6)
    cargar_job(redis, "j1", [png(original)], operaciones)

    split.manejador(redis, partes=4)(CanalQuePublica(), b'{"job_id": "j1"}')

    guardada = {k.decode(): v.decode() for k, v in redis.hgetall("job:j1:geometria:0").items()}
    assert guardada == {"total": "4", **geometria}
    assert [leer_fragmento(redis, "j1", 0, f).shape[0] for f in range(4)] == altos
    np.testing.assert_array_equal(reconstruir(redis, "j1", 0), original)


def test_una_imagen_con_menos_filas_que_partes_da_un_fragmento_por_fila(redis):
    original = imagen_rgb(2, 5)
    cargar_job(redis, "j1", [png(original)], cadena("sobel"))
    canal = CanalQuePublica()

    split.manejador(redis, partes=4)(canal, b'{"job_id": "j1"}')

    assert redis.hget("job:j1:geometria:0", "total") == b"2"
    assert sum(evento[0] == "publicar" for evento in canal.eventos) == 2
    np.testing.assert_array_equal(reconstruir(redis, "j1", 0), original)


def test_un_job_de_varios_archivos_reparte_todos(redis):
    originales = [imagen_rgb(9, 4, semilla=1), imagen_rgb(5, 7, semilla=2), imagen_rgb(12, 3, semilla=3)]
    cargar_job(redis, "j1", [png(o) for o in originales])
    canal = CanalQuePublica()

    split.manejador(redis, partes=3)(canal, b'{"job_id": "j1"}')

    for archivo, original in enumerate(originales):
        np.testing.assert_array_equal(reconstruir(redis, "j1", archivo), original)
    publicadas = [(e[2]["archivo"], e[2]["fragmento"]) for e in canal.eventos if e[0] == "publicar"]
    assert publicadas == [(a, f) for a in range(3) for f in range(3)]


def reloj(*instantes):
    """Devuelve los `instantes` en orden y después repite el último."""
    pendientes = list(instantes)
    return lambda: pendientes.pop(0) if len(pendientes) > 1 else pendientes[0]


def test_registra_el_total_de_fragmentos_del_job_y_lo_que_tardo_el_split(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(9, 4)), png(imagen_rgb(2, 4))])

    split.manejador(redis, partes=3, ahora_ms=reloj(1000, 1250))(CanalQuePublica(), b'{"job_id": "j1"}')

    assert redis.hget("job:j1", "fragmentos_total") == b"5"  # 3 del primero, 2 del segundo (tiene 2 filas)
    assert redis.hget("job:j1", "t_split_ms") == b"250"


@pytest.mark.parametrize("invalido", [b"no soy una imagen", png(imagen_rgb(40, 40))[:200]],
                         ids=["no_es_imagen", "png_cortado"])
def test_un_archivo_que_no_se_decodifica_deja_el_job_en_failed_sin_publicar_nada(redis, invalido):
    cargar_job(redis, "j1", [png(imagen_rgb(6, 4)), invalido])  # creado: 1761055391000
    canal = CanalQuePublica()

    split.manejador(redis, partes=2, ahora_ms=reloj(1761055392500))(canal, b'{"job_id": "j1"}')

    assert redis.hget("job:j1", "estado") == b"failed"
    assert json.loads(redis.hget("job:j1", "error")) == {
        "tipo": "ARCHIVO_INVALIDO", "mensaje": "el archivo 1 no se pudo decodificar"}
    assert redis.lrange("terminados", 0, -1) == [b"failed 1500"]
    assert canal.eventos == []


def test_terminados_guarda_solo_los_ultimos_1000(redis):
    redis.rpush("terminados", *[f"done {i}" for i in range(1000)])
    cargar_job(redis, "j1", [b"no soy una imagen"])

    split.manejador(redis, partes=2, ahora_ms=reloj(1761055392500))(CanalQuePublica(), b'{"job_id": "j1"}')

    assert redis.llen("terminados") == 1000
    assert redis.lindex("terminados", 0) == b"failed 1500"
    assert redis.lindex("terminados", -1) == b"done 998"


def test_un_archivo_que_pasa_el_doble_del_limite_de_pillow_tambien_deja_el_job_en_failed(redis, monkeypatch):
    # Con el límite real (100 MP) haría falta una imagen de más de 200 MP; bajarlo da el mismo error con 40×40.
    monkeypatch.setattr(Image, "MAX_IMAGE_PIXELS", 100)
    cargar_job(redis, "j1", [png(imagen_rgb(40, 40))])
    canal = CanalQuePublica()

    split.manejador(redis, partes=2, ahora_ms=reloj(1761055392500))(canal, b'{"job_id": "j1"}')

    assert redis.hget("job:j1", "estado") == b"failed"
    assert canal.eventos == []
