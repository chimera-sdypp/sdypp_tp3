import io
import json
import logging

import fakeredis
import numpy as np
import pika.exceptions
import pytest
from PIL import Image
from prometheus_client import REGISTRY

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
    assert redis.hget("job:j1", "terminado") == b"1761055392500"
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


def foto(redis):
    """Todo lo que hay en Redis (contenido y TTL), para comparar antes y después.

    No usa DUMP: en un Redis real, el orden interno de un hash grande cambia con las lecturas (rehash).
    """
    leer = {b"string": redis.get, b"hash": redis.hgetall, b"list": lambda c: redis.lrange(c, 0, -1)}
    return {clave: (leer[redis.type(clave)](clave), redis.ttl(clave)) for clave in redis.keys("*")}


@pytest.mark.parametrize("estado, con_entradas", [
    ("processing", True), ("failed", True), ("done", True),
    ("processing", False),  # redelivery después de borrar las entradas: no puede terminar en failed
])
def test_un_job_que_ya_no_esta_en_cola_no_se_reparte_de_nuevo(redis, estado, con_entradas):
    cargar_job(redis, "j1", [png(imagen_rgb(6, 4))])
    redis.hset("job:j1", "estado", estado)
    if not con_entradas:
        redis.delete("job:j1:entrada:0")
    antes = foto(redis)
    canal = CanalQuePublica()

    split.manejador(redis, partes=2)(canal, b'{"job_id": "j1"}')

    assert foto(redis) == antes
    assert canal.eventos == []


def test_un_job_inexistente_avisa_y_no_toca_nada(redis, caplog):
    redis.set("otra:clave", "sin relación")
    antes = foto(redis)
    canal = CanalQuePublica()

    split.manejador(redis, partes=2, log=logging.getLogger("test"))(canal, b'{"job_id": "vencido"}')

    assert foto(redis) == antes
    assert canal.eventos == []
    avisos = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert avisos == ["job vencido no existe (venció o el backend no lo terminó de crear), se descarta"]


def test_las_entradas_se_borran_despues_de_publicar(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(6, 4)), png(imagen_rgb(5, 4))])

    split.manejador(redis, partes=2)(CanalQuePublica(), b'{"job_id": "j1"}')

    assert redis.keys("job:j1:entrada:*") == []


class CanalQueSeCorta(CanalQuePublica):
    """Se cae RabbitMQ en la segunda publicación."""

    def basic_publish(self, exchange, routing_key, body, properties=None):
        if sum(e[0] == "publicar" for e in self.eventos) == 1:
            raise pika.exceptions.StreamLostError("se cortó la conexión")
        super().basic_publish(exchange, routing_key, body, properties)


def test_si_se_corta_al_publicar_el_job_se_puede_repetir_entero(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(6, 4))])

    with pytest.raises(pika.exceptions.StreamLostError):
        split.manejador(redis, partes=2)(CanalQueSeCorta(), b'{"job_id": "j1"}')

    assert redis.hget("job:j1", "estado") == b"queued"
    assert redis.exists("job:j1:entrada:0")
    canal = CanalQuePublica()
    split.manejador(redis, partes=2)(canal, b'{"job_id": "j1"}')  # el redelivery
    assert sum(e[0] == "publicar" for e in canal.eventos) == 2


@pytest.mark.parametrize("archivos", [
    [png(imagen_rgb(6, 4)), png(imagen_rgb(5, 4))],
    [png(imagen_rgb(6, 4)), b"no soy una imagen"],  # job failed: lo del archivo 0 tiene que vencer igual
], ids=["processing", "failed"])
def test_la_geometria_y_los_fragmentos_vencen_con_el_ttl(redis, archivos):
    cargar_job(redis, "j1", archivos)

    split.manejador(redis, partes=2, ttl=3600)(CanalQuePublica(), b'{"job_id": "j1"}')

    escritas = redis.keys("job:j1:frag:*") + redis.keys("job:j1:geometria:*")
    assert escritas
    assert {clave: redis.ttl(clave) for clave in escritas} == {clave: 3600 for clave in escritas}


class CanalConJoinerRapido(CanalQuePublica):
    """Con la última tarea publicada, el job ya termina (workers y joiner más rápidos que el split)."""

    def __init__(self, redis, tareas):
        super().__init__()
        self.redis, self.tareas = redis, tareas

    def basic_publish(self, exchange, routing_key, body, properties=None):
        super().basic_publish(exchange, routing_key, body, properties)
        if sum(e[0] == "publicar" for e in self.eventos) == self.tareas:
            self.redis.hset(f"job:{json.loads(body)['job_id']}", "estado", "done")


def test_si_el_job_termina_mientras_se_publica_el_split_no_pisa_done(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(6, 4))])

    split.manejador(redis, partes=2, ahora_ms=reloj(1000, 1300))(CanalConJoinerRapido(redis, 2), b'{"job_id": "j1"}')

    assert redis.hget("job:j1", "estado") == b"done"
    assert redis.hget("job:j1", "fragmentos_total") == b"2"
    assert redis.hget("job:j1", "t_split_ms") == b"300"
    assert redis.keys("job:j1:entrada:*") == []


def test_un_job_failed_tambien_borra_las_entradas(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(6, 4)), b"no soy una imagen"])

    split.manejador(redis, partes=2, ahora_ms=reloj(1761055392500))(CanalQuePublica(), b'{"job_id": "j1"}')

    assert redis.hget("job:j1", "estado") == b"failed"
    assert redis.keys("job:j1:entrada:*") == []


# Métricas (contrato §11). El registro de Prometheus es global: cada test mira cuánto sumó.

def metricas_del_split():
    def muestra(nombre, **etiquetas):
        return REGISTRY.get_sample_value(nombre, etiquetas) or 0
    return (muestra("sobel_tareas_procesadas_total", servicio="split", operacion="split", resultado="ok"),
            muestra("sobel_tareas_procesadas_total", servicio="split", operacion="split", resultado="error"),
            muestra("sobel_tiempo_tarea_segundos_sum", servicio="split", operacion="split"),
            muestra("sobel_errores_total", servicio="split", tipo="ARCHIVO_INVALIDO"))


def diferencia(antes):
    return [d - a for d, a in zip(metricas_del_split(), antes)]


def test_cada_job_partido_suma_uno_con_lo_que_tardo(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(9, 4))])
    antes = metricas_del_split()

    split.manejador(redis, partes=3, ahora_ms=reloj(1000, 1250))(CanalQuePublica(), b'{"job_id": "j1"}')

    assert diferencia(antes) == [1, 0, pytest.approx(0.25), 0]


def test_un_archivo_que_no_se_decodifica_cuenta_como_error_archivo_invalido(redis):
    cargar_job(redis, "j1", [b"no soy una imagen"])
    antes = metricas_del_split()

    split.manejador(redis, partes=2)(CanalQuePublica(), b'{"job_id": "j1"}')

    assert diferencia(antes) == [0, 1, 0, 1]


def test_un_job_que_no_existe_o_ya_se_repartio_no_cuenta(redis):
    cargar_job(redis, "j1", [png(imagen_rgb(4, 4))])
    redis.hset("job:j1", "estado", "processing")
    antes = metricas_del_split()

    for job_id in ("j1", "no-existe"):
        split.manejador(redis, partes=2)(CanalQuePublica(), json.dumps({"job_id": job_id}).encode())

    assert diferencia(antes) == [0, 0, 0, 0]


def test_un_job_que_va_a_la_dlq_cuenta_como_error(redis):
    antes = metricas_del_split()

    with pytest.raises(ValueError):
        split.manejador(redis, partes=2)(CanalQuePublica(), b"no es json")

    assert diferencia(antes) == [0, 1, 0, 0]
