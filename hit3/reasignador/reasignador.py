"""Reasignador: consume sobel.dlq y vuelve a encolar cada tarea que falló, con un intento más (contrato §9)."""

import json
import logging
import time

from comun import claves, colas

TAREA = "tarea"  # el origen de lo que vino de una sobel.tareas.<imagen>; los otros son sobel.split y sobel.joiner
CAMPOS_DE_TAREA = ("archivo", "fragmento", "paso", "operaciones", "intento")  # los del §9 que usa el reasignador


def ahora_en_ms():
    return int(time.time() * 1000)


class Ilegible(Exception):
    """Un mensaje de sobel.dlq que no se puede leer o que viene de una cola desconocida."""


def es_entero(valor):
    return isinstance(valor, int) and not isinstance(valor, bool)


def validar_tarea(tarea):
    faltan = [campo for campo in CAMPOS_DE_TAREA if campo not in tarea]
    if faltan:
        raise Ilegible(f"a la tarea le faltan {faltan}")
    if not isinstance(tarea["operaciones"], list) or not es_entero(tarea["paso"]) \
            or not 0 <= tarea["paso"] < len(tarea["operaciones"]):
        raise Ilegible(f"el paso {tarea['paso']!r} no está en la cadena")
    if not es_entero(tarea["intento"]) or tarea["intento"] < 1:
        raise Ilegible(f"el intento {tarea['intento']!r} no es un entero desde 1")
    try:
        imagen_del_paso(tarea)
    except (KeyError, TypeError) as error:
        raise Ilegible(f"el paso no tiene imagen ({error!r})") from error


def leer(cuerpo, propiedades):
    """Devuelve (origen, mensaje, motivo): el origen es TAREA, sobel.split o sobel.joiner, según
    `x-first-death-queue` (contrato §9), y el motivo, `x-first-death-reason` (rejected o delivery_limit)."""
    try:
        mensaje = json.loads(cuerpo)
        headers = propiedades.headers or {}
        cola, motivo = headers["x-first-death-queue"], headers.get("x-first-death-reason", "?")
    except (ValueError, KeyError, AttributeError) as error:
        raise Ilegible(f"{error!r} en {cuerpo[:200]!r}") from error
    if not isinstance(mensaje, dict) or "job_id" not in mensaje:
        raise Ilegible(f"sin job_id: {cuerpo[:200]!r}")
    if cola.startswith(colas.PREFIJO_TAREAS):
        validar_tarea(mensaje)
        return TAREA, mensaje, motivo
    if cola not in (colas.SPLIT, colas.JOINER):
        raise Ilegible(f"viene de {cola}, una cola sin DLX")
    return cola, mensaje, motivo


def imagen_del_paso(tarea):
    return tarea["operaciones"][tarea["paso"]]["imagen"]


def reintentar(canal, redis, tarea, ahora_ms):
    """Vuelve a encolar la tarea en la cola de la imagen de su paso, sin espera, con un intento más. Devuelve el
    intento nuevo.

    Publica antes de sumar `reintentos`: si se cae en el medio, la reentrega publica de nuevo y el worker soporta el
    duplicado (contrato §9); perder la tarea sería peor que contarla de más. Si el broker rechaza la publicación
    (NackError), `consumir` hace nack sin reencolar y la tarea se pierde con el error en el log: reencolarla podría
    ciclar para siempre, porque un nack que reencola no cuenta para el límite de entregas.
    """
    intento = tarea["intento"] + 1
    colas.publicar(canal, colas.declarar_tareas(canal, imagen_del_paso(tarea)),
                   {**tarea, "intento": intento, "encolado": ahora_ms()})
    clave_job = claves.job(tarea["job_id"])

    def sumar(transaccion):
        if transaccion.exists(clave_job):  # si venció justo ahora, un HINCRBY lo recrearía sin TTL
            transaccion.multi()
            transaccion.hincrby(clave_job, "reintentos", 1)

    redis.transaction(sumar, clave_job)
    return intento


def manejador(redis, reintentos_max, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.reasignador")):
    """Devuelve el callback(canal, cuerpo, propiedades) que `consumir` llama por cada mensaje de sobel.dlq."""

    def procesar(canal, cuerpo, propiedades):
        try:
            origen, mensaje, motivo = leer(cuerpo, propiedades)
        except Ilegible as error:
            # sobel.dlq no tiene DLX: un nack lo descartaría igual, así que se descarta con el detalle en el log.
            log.error("mensaje ilegible en %s, se descarta: %s", colas.DLQ, error)
            return
        job_id = mensaje["job_id"]
        if not redis.exists(claves.job(job_id)):
            # Ni reintentarlo ni marcar el job lo arreglan: venció.
            log.warning("job %s no existe, se descarta un mensaje de %s", job_id, origen)
            return
        if origen == TAREA:
            intento = reintentar(canal, redis, mensaje, ahora_ms)
            log.info("job %s: archivo %d, fragmento %d, paso %d (%s): intento %d", job_id, mensaje["archivo"],
                     mensaje["fragmento"], mensaje["paso"], motivo, intento)
        else:
            log.warning("job %s: llegó un mensaje de %s a la DLQ (%s), se descarta", job_id, origen, motivo)

    return procesar
