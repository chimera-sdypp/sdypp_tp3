import io
import json

import fakeredis
import numpy as np
import pytest
from PIL import Image

from split import split
from worker import worker

IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"


def cadena(*operaciones):
    return [{"operacion": op, "imagen": IMAGEN} for op in operaciones]


class CanalQuePublica:
    """Lo que el worker usa de un canal de pika: declarar colas y publicar (a una cola o a un exchange), en orden."""

    def __init__(self):
        self.eventos = []

    def queue_declare(self, queue, durable=False, arguments=None):
        self.eventos.append(("declarar", queue))

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.eventos.append(("publicar", exchange, routing_key, json.loads(body)))


def png(matriz):
    salida = io.BytesIO()
    Image.fromarray(matriz).save(salida, format="PNG")
    return salida.getvalue()


def imagen_rgb(alto, ancho, semilla=0):
    return np.random.default_rng(semilla).integers(0, 256, (alto, ancho, 3), dtype=np.uint8)


def leer(datos):
    return np.asarray(Image.open(io.BytesIO(datos)))


def reloj(*instantes):
    pendientes = iter(instantes)
    return lambda: next(pendientes)


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


def cargar_tarea(redis, fragmento, operaciones=cadena("grises"), paso=0, job_id="j1", archivo=0, indice=2,
                 encolado=1761055391000, intento=1):
    """Deja la entrada como la deja el split (o el worker del paso anterior) y devuelve el cuerpo de la tarea (§9)."""
    redis.set(f"job:{job_id}:frag:{archivo}:{indice}:{paso}", png(fragmento))
    return json.dumps({"job_id": job_id, "archivo": archivo, "fragmento": indice, "paso": paso,
                       "operaciones": operaciones, "intento": intento, "encolado": encolado}).encode()


def manejador(redis, **opciones):
    opciones = {"nombre": "w1", "ahora_ms": reloj(1761055391100, 1761055391512), **opciones}
    return worker.manejador(redis, **opciones)


def test_grises_deja_el_fragmento_en_un_canal_en_el_paso_siguiente(redis):
    fragmento = imagen_rgb(4, 6)
    cuerpo = cargar_tarea(redis, fragmento)

    manejador(redis)(CanalQuePublica(), cuerpo)

    esperado = np.asarray(Image.fromarray(fragmento).convert("L"))
    np.testing.assert_array_equal(leer(redis.get("job:j1:frag:0:2:1")), esperado)


def test_el_ultimo_paso_publica_el_resultado_en_el_fanout(redis):
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6), intento=2)
    canal = CanalQuePublica()

    manejador(redis)(canal, cuerpo)

    assert canal.eventos == [("publicar", "sobel.resultados", "", {
        "job_id": "j1", "archivo": 0, "fragmento": 2, "worker": "w1", "ms": 412, "intento": 2})]


def test_suma_sus_tiempos_a_los_del_job_y_se_anota_entre_sus_workers(redis):
    redis.hset("job:j1", mapping={"cola_ms": 50, "cola_n": 2, "proc_ms": 1000, "proc_n": 2})
    redis.sadd("job:j1:workers", "w0")
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6), encolado=1761055391000)  # lo toma a los 100 ms; tarda 412

    manejador(redis)(CanalQuePublica(), cuerpo)

    assert redis.hmget("job:j1", "cola_ms", "cola_n", "proc_ms", "proc_n") == [b"150", b"3", b"1412", b"3"]
    assert redis.smembers("job:j1:workers") == {b"w0", b"w1"}


def test_borra_la_entrada_al_terminar(redis):
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6))

    manejador(redis)(CanalQuePublica(), cuerpo)

    assert not redis.exists("job:j1:frag:0:2:0")


def test_la_salida_y_los_workers_del_job_vencen_con_el_ttl(redis):
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6))

    manejador(redis, ttl=600)(CanalQuePublica(), cuerpo)

    for clave in ("job:j1:frag:0:2:1", "job:j1:workers"):
        assert 0 < redis.ttl(clave) <= 600, clave


def test_grises_sobre_un_fragmento_de_un_canal_lo_deja_igual(redis):
    fragmento = imagen_rgb(4, 6)[:, :, 0]
    cuerpo = cargar_tarea(redis, fragmento)

    manejador(redis)(CanalQuePublica(), cuerpo)

    np.testing.assert_array_equal(leer(redis.get("job:j1:frag:0:2:1")), fragmento)


# Encadenado con el split (seams S4 + S6): el job entra como lo deja el backend y sale como lo une el joiner.

def cargar_job(redis, job_id, archivos, operaciones):
    """Deja el job como lo deja el backend (contrato §10) antes de publicarlo en sobel.split."""
    redis.hset(f"job:{job_id}", mapping={
        "usuario": "ana", "estado": "queued", "creado": 1761055391000,
        "operaciones": json.dumps(operaciones), "archivos": len(archivos),
    })
    for a, datos in enumerate(archivos):
        redis.set(f"job:{job_id}:entrada:{a}", datos)


def procesar_job(redis, job_id, partes):
    """Corre el split y después el worker sobre cada tarea publicada, hasta que no quedan tareas.

    Devuelve los mensajes que llegaron a sobel.resultados.
    """
    canal = CanalQuePublica()
    split.manejador(redis, partes=partes)(canal, json.dumps({"job_id": job_id}).encode())
    resultados = []
    trabajador = worker.manejador(redis, nombre="w1")
    while canal.eventos:
        evento = canal.eventos.pop(0)
        if evento[0] != "publicar":
            continue
        _, exchange, _, mensaje = evento
        if exchange == "sobel.resultados":
            resultados.append(mensaje)
        else:
            trabajador(canal, json.dumps(mensaje).encode())
    return resultados


def unir(redis, job_id, archivo, paso):
    """Une los fragmentos del `paso` recortando los bordes según la geometría (§10), como lo hará el joiner."""
    geometria = {k.decode(): v.decode() for k, v in redis.hgetall(f"job:{job_id}:geometria:{archivo}").items()}
    franjas = []
    for f in range(int(geometria["total"])):
        extra, filas = map(int, geometria[str(f)].split(","))
        franjas.append(leer(redis.get(f"job:{job_id}:frag:{archivo}:{f}:{paso}"))[extra:extra + filas])
    return np.vstack(franjas)


def test_un_job_en_grises_partido_y_procesado_da_la_imagen_entera_en_grises(redis):
    originales = [imagen_rgb(11, 7, semilla=1), imagen_rgb(5, 9, semilla=2)]
    cargar_job(redis, "j1", [png(o) for o in originales], cadena("grises"))

    resultados = procesar_job(redis, "j1", partes=3)

    assert sorted((r["archivo"], r["fragmento"]) for r in resultados) == [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
    for archivo, original in enumerate(originales):
        esperado = np.asarray(Image.fromarray(original).convert("L"))
        np.testing.assert_array_equal(unir(redis, "j1", archivo, paso=1), esperado)
