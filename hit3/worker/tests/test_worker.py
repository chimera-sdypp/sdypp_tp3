import io
import json
import signal
import time

import fakeredis
import numpy as np
import pytest
from PIL import Image

from split import split
from worker import worker
from worker.operaciones import OPERACIONES

IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"
OTRA_IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.1.0"


def cadena(*operaciones):
    return [{"operacion": op, "imagen": IMAGEN} for op in operaciones]


def grises_en(*imagenes):
    """Una cadena de `grises` con un paso por imagen de worker: sirve para seguir a qué cola va cada paso."""
    return [{"operacion": "grises", "imagen": imagen} for imagen in imagenes]


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
    """Deja el job y la entrada como los dejan el backend y el split (o el worker del paso anterior).

    Devuelve el cuerpo de la tarea (§9).
    """
    redis.hsetnx(f"job:{job_id}", "estado", "processing")
    redis.expire(f"job:{job_id}", 86400)
    redis.set(f"job:{job_id}:frag:{archivo}:{indice}:{paso}", png(fragmento))
    return json.dumps({"job_id": job_id, "archivo": archivo, "fragmento": indice, "paso": paso,
                       "operaciones": operaciones, "intento": intento, "encolado": encolado}).encode()


def manejador(redis, **opciones):
    # Recibe la tarea, 200 ms después (leer y decodificar) empieza la operación, que tarda 412 ms.
    opciones = {"nombre": "w1", "ahora_ms": reloj(1761055391100, 1761055391300, 1761055391712), **opciones}
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


def test_un_paso_intermedio_manda_la_tarea_a_la_cola_de_la_imagen_del_paso_siguiente(redis):
    operaciones = grises_en(IMAGEN, OTRA_IMAGEN)
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6), operaciones=operaciones, intento=3)
    canal = CanalQuePublica()
    # Los tres instantes de siempre y uno más: cuando publica la tarea del paso siguiente.
    ahora_ms = reloj(1761055391100, 1761055391300, 1761055391712, 1761055391720)

    manejador(redis, ahora_ms=ahora_ms)(canal, cuerpo)

    cola = f"sobel.tareas.{OTRA_IMAGEN}"
    assert canal.eventos == [("declarar", cola), ("publicar", "", cola, {
        "job_id": "j1", "archivo": 0, "fragmento": 2, "paso": 1, "operaciones": operaciones,
        "intento": 1, "encolado": 1761055391720})]
    assert redis.exists("job:j1:frag:0:2:1") and not redis.exists("job:j1:frag:0:2:0")


def test_el_ultimo_paso_de_una_cadena_publica_el_resultado_con_el_intento_de_esa_tarea(redis):
    operaciones = grises_en(IMAGEN, OTRA_IMAGEN)
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6), operaciones=operaciones, paso=1, intento=2)
    canal = CanalQuePublica()

    manejador(redis)(canal, cuerpo)

    assert canal.eventos == [("publicar", "sobel.resultados", "", {
        "job_id": "j1", "archivo": 0, "fragmento": 2, "worker": "w1", "ms": 412, "intento": 2})]
    assert redis.exists("job:j1:frag:0:2:2") and not redis.exists("job:j1:frag:0:2:1")


def test_suma_sus_tiempos_a_los_del_job_y_se_anota_entre_sus_workers(redis):
    redis.hset("job:j1", mapping={"cola_ms": 50, "cola_n": 2, "proc_ms": 1000, "proc_n": 2})
    redis.sadd("job:j1:workers", "w0")
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6), encolado=1761055391000)  # espera 100 ms en la cola; tarda 412

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

    Cada tarea tiene que ir, ya declarada, a la cola de la imagen de su paso. Devuelve los mensajes que
    llegaron a sobel.resultados.
    """
    canal = CanalQuePublica()
    split.manejador(redis, partes=partes)(canal, json.dumps({"job_id": job_id}).encode())
    resultados = []
    declaradas = set()
    trabajador = worker.manejador(redis, nombre="w1")
    while canal.eventos:
        evento = canal.eventos.pop(0)
        if evento[0] == "declarar":
            declaradas.add(evento[1])
            continue
        _, exchange, cola, mensaje = evento
        if exchange == "sobel.resultados":
            resultados.append(mensaje)
            continue
        assert cola == f"sobel.tareas.{mensaje['operaciones'][mensaje['paso']]['imagen']}" and cola in declaradas
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

    assert sorted((r["archivo"], r["fragmento"]) for r in resultados) == [(a, f) for a in (0, 1) for f in (0, 1, 2)]
    for archivo, original in enumerate(originales):
        esperado = np.asarray(Image.fromarray(original).convert("L"))
        np.testing.assert_array_equal(unir(redis, "j1", archivo, paso=1), esperado)


@pytest.mark.parametrize("imagenes", [[IMAGEN, OTRA_IMAGEN], [OTRA_IMAGEN, IMAGEN, OTRA_IMAGEN]],
                         ids=["2_pasos", "3_pasos"])
def test_una_cadena_de_varios_pasos_llega_entera_a_los_resultados(redis, imagenes):
    original = imagen_rgb(10, 6, semilla=3)
    cargar_job(redis, "j1", [png(original)], grises_en(*imagenes))

    resultados = procesar_job(redis, "j1", partes=4)

    assert sorted(r["fragmento"] for r in resultados) == [0, 1, 2, 3]
    np.testing.assert_array_equal(unir(redis, "j1", 0, paso=len(imagenes)),
                                  np.asarray(Image.fromarray(original).convert("L")))
    # Sólo quedan los del último paso: cada paso borró su entrada.
    assert sorted(redis.keys("job:j1:frag:*")) == [f"job:j1:frag:0:{f}:{len(imagenes)}".encode() for f in range(4)]


def aplicar_entera(original, operaciones):
    """La cadena aplicada a la imagen entera, sin partir: la referencia del procesamiento distribuido.

    Usa las mismas operaciones que el worker a propósito: este test mide que partir y recortar los bordes no cambie
    nada. Que cada operación esté bien lo prueban los tests de operaciones (y el Sobel, contra el Hit 1).
    """
    resultado = original
    for paso in operaciones:
        resultado = OPERACIONES[paso["operacion"]](resultado)
    return resultado


@pytest.mark.parametrize("nombres", [
    ["grises"],  # ninguna operación de vecindad: sin filas de borde
    ["sobel"], ["blur"], ["grises", "blur"],  # una operación de vecindad: una fila de borde
    ["blur", "sobel"], ["blur", "grises", "sobel"],  # dos: dos filas de borde
], ids=lambda nombres: "_".join(nombres))
def test_partir_y_procesar_da_lo_mismo_que_aplicar_la_cadena_a_la_imagen_entera(redis, nombres):
    originales = [imagen_rgb(13, 8, semilla=4), imagen_rgb(7, 5, semilla=5)]  # 7 filas en 4 partes: 1 o 2 por fragmento
    cargar_job(redis, "j1", [png(o) for o in originales], cadena(*nombres))

    procesar_job(redis, "j1", partes=4)

    for archivo, original in enumerate(originales):
        np.testing.assert_array_equal(unir(redis, "j1", archivo, paso=len(nombres)),
                                      aplicar_entera(original, cadena(*nombres)))


def test_sobel_distribuido_es_identico_al_centralizado_del_hit1(redis, sobel_hit1):
    original = imagen_rgb(17, 11, semilla=6)
    cargar_job(redis, "j1", [png(original)], cadena("sobel"))

    procesar_job(redis, "j1", partes=4)

    np.testing.assert_array_equal(unir(redis, "j1", 0, paso=1), sobel_hit1.aplicar(sobel_hit1.leer_gris(png(original))))


# Anuncio de worker activo (seam S8).

def test_el_anuncio_deja_la_imagen_y_las_operaciones_del_worker_por_15_segundos(redis):
    worker.anunciar(redis, "w1", IMAGEN, ["sobel", "grises"])

    assert redis.hgetall("worker:w1") == {b"imagen": IMAGEN.encode(), b"operaciones": b"sobel,grises", b"host": b"w1"}
    assert 0 < redis.ttl("worker:w1") <= 15


# Cuando algo sale mal (ticket 05): redelivery, job vencido, errores y timeout.

def test_un_redelivery_de_una_tarea_ya_terminada_vuelve_a_mandar_el_paso_siguiente_sin_sumar_tiempos(redis):
    # Se cortó entre el borrado de la entrada y el ack: la salida quedó escrita y la tarea vuelve a llegar.
    operaciones = grises_en(IMAGEN, OTRA_IMAGEN)
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6), operaciones=operaciones)
    redis.rename("job:j1:frag:0:2:0", "job:j1:frag:0:2:1")
    antes = redis.hgetall("job:j1")
    canal = CanalQuePublica()

    manejador(redis, ahora_ms=reloj(1761055391100, 1761055391120))(canal, cuerpo)

    cola = f"sobel.tareas.{OTRA_IMAGEN}"
    assert canal.eventos == [("declarar", cola), ("publicar", "", cola, {
        "job_id": "j1", "archivo": 0, "fragmento": 2, "paso": 1, "operaciones": operaciones,
        "intento": 1, "encolado": 1761055391120})]
    assert redis.hgetall("job:j1") == antes and not redis.exists("job:j1:workers")


def test_un_redelivery_del_ultimo_paso_vuelve_a_publicar_el_resultado_sin_sumar_tiempos(redis):
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6), intento=2)
    redis.rename("job:j1:frag:0:2:0", "job:j1:frag:0:2:1")
    canal = CanalQuePublica()

    manejador(redis, ahora_ms=reloj(1761055391100))(canal, cuerpo)

    # ms 0: este worker no sabe cuánto tardó la vez que se procesó (contrato §9).
    assert canal.eventos == [("publicar", "sobel.resultados", "", {
        "job_id": "j1", "archivo": 0, "fragmento": 2, "worker": "w1", "ms": 0, "intento": 2})]
    assert not redis.hexists("job:j1", "proc_n") and not redis.exists("job:j1:workers")

def test_una_tarea_sin_entrada_ni_salida_avisa_y_no_hace_nada(redis, caplog):
    # Por ejemplo, el joiner ya unió el archivo y borró los fragmentos del último paso.
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6))
    redis.delete("job:j1:frag:0:2:0")
    antes = redis.hgetall("job:j1")
    canal = CanalQuePublica()

    manejador(redis)(canal, cuerpo)

    assert canal.eventos == []
    assert redis.hgetall("job:j1") == antes and redis.keys("job:j1:*") == []
    assert any(r.levelname == "WARNING" and "j1" in r.getMessage() for r in caplog.records)


def test_una_tarea_de_un_job_vencido_no_lo_revive(redis, caplog):
    # Las claves vencen en momentos distintos: el hash del job ya no está, pero el fragmento sí.
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6))
    redis.delete("job:j1")
    canal = CanalQuePublica()

    manejador(redis)(canal, cuerpo)

    assert canal.eventos == []
    assert not redis.exists("job:j1") and not redis.exists("job:j1:workers")
    assert any(r.levelname == "WARNING" and "j1" in r.getMessage() for r in caplog.records)


def test_una_operacion_que_el_worker_no_ofrece_va_a_la_dlq_sin_publicar_nada(redis):
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6), operaciones=cadena("grises"))
    canal = CanalQuePublica()

    with pytest.raises(worker.OperacionNoOfrecida, match="grises"):  # consumir hace nack: va a la DLQ
        manejador(redis, ofrecidas=["sobel", "blur"])(canal, cuerpo)

    assert canal.eventos == []
    assert redis.exists("job:j1:frag:0:2:0") and not redis.exists("job:j1:frag:0:2:1")  # el reasignador la reintenta


@pytest.mark.parametrize("datos", [b"no soy un png", png(imagen_rgb(8, 8))[:60]], ids=["no_es_png", "png_cortado"])
def test_un_fragmento_que_no_se_decodifica_va_a_la_dlq_sin_publicar_nada(redis, datos):
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6))
    redis.set("job:j1:frag:0:2:0", datos)
    canal = CanalQuePublica()

    with pytest.raises(ValueError, match="decodificar"):
        manejador(redis)(canal, cuerpo)

    assert canal.eventos == [] and not redis.exists("job:j1:frag:0:2:1")


def lenta(matriz):
    for _ in range(100):  # 5 s en total, en pasos cortos como las ventanas del Sobel
        time.sleep(0.05)
    return matriz


def test_una_operacion_que_pasa_el_timeout_se_corta_y_va_a_la_dlq_sin_publicar_nada(redis):
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6))
    canal = CanalQuePublica()
    handler_anterior = signal.getsignal(signal.SIGALRM)
    inicio = time.monotonic()

    with pytest.raises(worker.TiempoAgotado):
        manejador(redis, funciones={"grises": lenta}, timeout=0.2)(canal, cuerpo)

    assert time.monotonic() - inicio < 2
    assert canal.eventos == [] and not redis.exists("job:j1:frag:0:2:1")
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
    assert signal.getsignal(signal.SIGALRM) is handler_anterior


def test_una_operacion_que_termina_a_tiempo_deja_el_timer_desarmado(redis):
    cuerpo = cargar_tarea(redis, imagen_rgb(4, 6))
    handler_anterior = signal.getsignal(signal.SIGALRM)

    manejador(redis, timeout=30)(CanalQuePublica(), cuerpo)

    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
    assert signal.getsignal(signal.SIGALRM) is handler_anterior
