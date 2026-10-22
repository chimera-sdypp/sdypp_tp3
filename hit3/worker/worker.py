"""Worker: toma cada tarea de sobel.tareas.<imagen>, aplica la operación del paso y manda el fragmento al paso siguiente."""

import io
import json
import logging
import time

import numpy as np
from PIL import Image

from comun import claves, colas, pixeles
from worker.operaciones import OPERACIONES

TTL_POR_DEFECTO = 86400  # SOBEL_TTL: las claves de un job viven 24 h (contrato §10)


def ahora_en_ms():
    return int(time.time() * 1000)


def leer_png(datos):
    """Bytes PNG → matriz uint8 con los canales que tenga (sin forzar RGB, a diferencia del split)."""
    with Image.open(io.BytesIO(datos)) as imagen:
        return np.asarray(imagen)


def manejador(redis, nombre, operaciones=OPERACIONES, ttl=TTL_POR_DEFECTO, ahora_ms=ahora_en_ms,
              log=logging.getLogger("tp3.worker")):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada tarea; lo que escribe vence en `ttl` s."""

    def procesar(canal, cuerpo):
        tarea = json.loads(cuerpo)
        job_id, archivo, indice, paso = tarea["job_id"], tarea["archivo"], tarea["fragmento"], tarea["paso"]
        operacion = tarea["operaciones"][paso]["operacion"]
        entrada = leer_png(redis.get(claves.fragmento(job_id, archivo, indice, paso)))
        inicio = ahora_ms()
        salida = operaciones[operacion](entrada)
        proc_ms = ahora_ms() - inicio
        redis.set(claves.fragmento(job_id, archivo, indice, paso + 1), pixeles.a_png(salida), ex=ttl)
        colas.publicar(canal, "", {"job_id": job_id, "archivo": archivo, "fragmento": indice, "worker": nombre,
                                   "ms": proc_ms, "intento": tarea["intento"]}, exchange=colas.RESULTADOS)
        with redis.pipeline() as transaccion:  # sumas y cantidades para los promedios de tiempos_s (contrato §3)
            transaccion.hincrby(claves.job(job_id), "cola_ms", inicio - tarea["encolado"])
            transaccion.hincrby(claves.job(job_id), "cola_n", 1)
            transaccion.hincrby(claves.job(job_id), "proc_ms", proc_ms)
            transaccion.hincrby(claves.job(job_id), "proc_n", 1)
            transaccion.sadd(claves.workers(job_id), nombre)
            transaccion.expire(claves.workers(job_id), ttl)
            transaccion.execute()
        # Antes del ack, que `consumir` hace al volver: si se corta entre los dos, el redelivery no encuentra la
        # entrada (contrato §9, paso 4).
        redis.delete(claves.fragmento(job_id, archivo, indice, paso))

    return procesar
