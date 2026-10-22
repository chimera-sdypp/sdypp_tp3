import io
import json

import fakeredis
import numpy as np
import pytest
from PIL import Image

from joiner import joiner
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


def reloj(*instantes):
    pendientes = iter(instantes)
    return lambda: next(pendientes)


class Canal:
    """El joiner no publica nada: sólo recibe el canal porque `consumir` se lo pasa."""


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


def preparar_job(redis, archivos=1):
    """Deja el job de una cadena de un paso (grises) en processing, como lo dejan el backend y el split."""
    redis.hset("job:j1", mapping={
        "usuario": "ana", "estado": "processing", "creado": CREADO, "archivos": archivos,
        "operaciones": json.dumps([{"operacion": "grises", "imagen": IMAGEN}]),
        "fragmentos_total": 3 * archivos, "completos": 0})
    redis.expire("job:j1", 86400)


def preparar_archivo(redis, archivo=0, corrimiento=0):
    """Deja un archivo de 6 × 4 partido en 3 fragmentos de 2 filas, sin bordes, y ya procesado (paso 1), como lo
    dejan el split y el worker. `corrimiento` cambia los píxeles, para distinguir archivos. Devuelve la imagen."""
    imagen = (np.arange(24, dtype=np.uint8).reshape(6, 4) + corrimiento) * 10
    redis.hset(f"job:j1:geometria:{archivo}", mapping={"total": 3, "0": "0,2", "1": "0,2", "2": "0,2"})
    for f in range(3):
        redis.set(f"job:j1:frag:{archivo}:{f}:1", png(imagen[2 * f:2 * f + 2]))
    return imagen


def preparar_archivo_terminado(redis):
    """Un job de un solo archivo, listo para unir. Devuelve la imagen."""
    preparar_job(redis)
    return preparar_archivo(redis)


def mensaje_resultado(job_id="j1", archivo=0, fragmento=0):
    """Lo que publica el worker en sobel.resultados (contrato §9)."""
    return json.dumps({"job_id": job_id, "archivo": archivo, "fragmento": fragmento, "worker": "w1",
                       "ms": 12, "intento": 1}).encode()


# Dos uniones y un cierre: la primera unión tarda 40 ms, la segunda 30 y el cierre es a las …96050.
DOS_UNIONES = (1761055395000, 1761055395040, 1761055396000, 1761055396030, 1761055396050)


def manejador(redis, **opciones):
    opciones = {"ahora_ms": reloj(1761055395000, 1761055395040, 1761055395050), **opciones}
    return joiner.manejador(redis, **opciones)


def test_un_fragmento_completo_se_cuenta_y_todavia_no_se_une(redis):
    preparar_archivo_terminado(redis)

    manejador(redis)(Canal(), mensaje_resultado(fragmento=1))

    assert redis.smembers("job:j1:listos:0") == {b"1"}
    assert redis.hget("job:j1", "completos") == b"1"
    assert not redis.exists("job:j1:resultado:0") and redis.hget("job:j1", "estado") == b"processing"


def test_con_el_ultimo_fragmento_une_el_archivo_lo_marca_y_borra_el_ultimo_paso(redis):
    imagen = preparar_archivo_terminado(redis)
    procesar = manejador(redis)

    for f in (2, 0, 1):  # en cualquier orden
        procesar(Canal(), mensaje_resultado(fragmento=f))

    np.testing.assert_array_equal(leer(redis.get("job:j1:resultado:0")), imagen)
    assert redis.smembers("job:j1:unidos") == {b"0"}
    assert redis.keys("job:j1:frag:*") == []


def test_con_todos_los_archivos_unidos_el_job_pasa_a_done(redis):
    preparar_archivo_terminado(redis)
    procesar = manejador(redis)  # une de 1761055395000 a …040 (40 ms) y cierra a las …050

    for f in range(3):
        procesar(Canal(), mensaje_resultado(fragmento=f))

    assert redis.hmget("job:j1", "estado", "terminado", "t_join_ms", "completos") == [
        b"done", b"1761055395050", b"40", b"3"]
    assert redis.lrange("terminados", 0, -1) == [b"done 4050"]  # terminado − creado


def test_listos_unidos_y_el_resultado_vencen_con_el_ttl(redis):
    preparar_archivo_terminado(redis)
    procesar = manejador(redis, ttl=600)

    for f in range(3):
        procesar(Canal(), mensaje_resultado(fragmento=f))

    for clave in ("job:j1:listos:0", "job:j1:unidos", "job:j1:resultado:0"):
        assert 0 < redis.ttl(clave) <= 600, clave


def test_un_archivo_completo_se_une_sin_esperar_a_los_otros_y_el_job_cierra_con_el_ultimo(redis):
    preparar_job(redis, archivos=2)
    primero = preparar_archivo(redis, 0)
    segundo = preparar_archivo(redis, 1, corrimiento=1)
    procesar = manejador(redis, ahora_ms=reloj(*DOS_UNIONES))

    for f in range(3):
        procesar(Canal(), mensaje_resultado(archivo=1, fragmento=f))

    np.testing.assert_array_equal(leer(redis.get("job:j1:resultado:1")), segundo)
    assert not redis.exists("job:j1:resultado:0") and redis.hget("job:j1", "estado") == b"processing"

    for f in range(3):
        procesar(Canal(), mensaje_resultado(archivo=0, fragmento=f))

    np.testing.assert_array_equal(leer(redis.get("job:j1:resultado:0")), primero)
    assert redis.hget("job:j1", "estado") == b"done"
    assert redis.lrange("terminados", 0, -1) == [b"done 5050"]


def test_t_join_ms_suma_lo_que_tardo_cada_union(redis):
    preparar_job(redis, archivos=2)
    preparar_archivo(redis, 0)
    preparar_archivo(redis, 1, corrimiento=1)
    procesar = manejador(redis, ahora_ms=reloj(*DOS_UNIONES))

    for archivo in (0, 1):
        for f in range(3):
            procesar(Canal(), mensaje_resultado(archivo=archivo, fragmento=f))

    assert redis.hget("job:j1", "t_join_ms") == b"70"


# Encadenado con el split y el worker (seams S4 + S6 + S9): el job entra como lo deja el backend y sale en done.

class CanalQuePublica:
    """Lo que el split y el worker usan de un canal de pika: declarar colas y publicar, en orden."""

    def __init__(self):
        self.eventos = []

    def queue_declare(self, queue, durable=False, arguments=None):
        pass

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.eventos.append((exchange, body))


def cargar_job(redis, job_id, archivos, operaciones):
    """Deja el job como lo deja el backend (contrato §10) antes de publicarlo en sobel.split."""
    redis.hset(f"job:{job_id}", mapping={
        "usuario": "ana", "estado": "queued", "creado": CREADO, "operaciones": json.dumps(operaciones),
        "archivos": len(archivos), "fragmentos_total": 0, "completos": 0})
    for a, datos in enumerate(archivos):
        redis.set(f"job:{job_id}:entrada:{a}", datos)


def procesar_job(redis, job_id, partes):
    """Split, después el worker sobre cada tarea y el joiner sobre cada fragmento completo, hasta que no queda nada."""
    canal = CanalQuePublica()
    split.manejador(redis, partes=partes)(canal, json.dumps({"job_id": job_id}).encode())
    trabajador, uniones = worker.manejador(redis, nombre="w1"), joiner.manejador(redis)
    while canal.eventos:
        exchange, cuerpo = canal.eventos.pop(0)
        (uniones if exchange == "sobel.resultados" else trabajador)(canal, cuerpo)


def aplicar_entera(original, operaciones):
    """La cadena aplicada a la imagen entera, sin partir: la referencia del procesamiento distribuido.

    Usa las operaciones del worker a propósito: mide que partir, procesar y unir no cambie nada. Que cada operación
    esté bien lo prueban los tests del worker.
    """
    for paso in operaciones:
        original = OPERACIONES[paso["operacion"]](original)
    return original


@pytest.mark.parametrize("nombres", [
    ["grises"],  # ninguna operación de vecindad: sin filas de borde
    ["sobel"], ["grises", "blur"],  # una operación de vecindad: una fila de borde
    ["blur", "sobel"], ["blur", "grises", "sobel"],  # dos: dos filas de borde
], ids=lambda nombres: "_".join(nombres))
def test_cada_archivo_del_job_sale_igual_que_aplicarle_la_cadena_entero(redis, nombres):
    generador = np.random.default_rng(9)
    # Alturas distintas, para que cada archivo tenga su propia geometría: 13 filas en 4 partes son 3 o 4 por fragmento,
    # 7 son 1 o 2 y 3 dan menos filas que partes.
    originales = [generador.integers(0, 256, (alto, 6, 3), dtype=np.uint8) for alto in (13, 7, 3)]
    operaciones = [{"operacion": op, "imagen": IMAGEN} for op in nombres]
    cargar_job(redis, "j1", [png(o) for o in originales], operaciones)

    procesar_job(redis, "j1", partes=4)

    assert redis.hget("job:j1", "estado") == b"done"
    # 13 y 7 filas dan 4 fragmentos cada una y 3 filas dan 3: uno por fila.
    assert redis.hmget("job:j1", "fragmentos_total", "completos") == [b"11", b"11"]
    for archivo, original in enumerate(originales):
        np.testing.assert_array_equal(leer(redis.get(f"job:j1:resultado:{archivo}")),
                                      aplicar_entera(original, operaciones))
    assert redis.keys("job:j1:frag:*") == []


def test_con_sobel_solo_cada_archivo_es_identico_al_sobel_centralizado_del_hit1(redis, sobel_hit1):
    generador = np.random.default_rng(10)
    originales = [generador.integers(0, 256, (alto, 9, 3), dtype=np.uint8) for alto in (17, 6)]
    cargar_job(redis, "j1", [png(o) for o in originales], [{"operacion": "sobel", "imagen": IMAGEN}])

    procesar_job(redis, "j1", partes=4)

    for archivo, original in enumerate(originales):
        np.testing.assert_array_equal(leer(redis.get(f"job:j1:resultado:{archivo}")),
                                      sobel_hit1.aplicar(sobel_hit1.leer_gris(png(original))))
