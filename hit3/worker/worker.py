"""Worker: toma cada tarea de sobel.tareas.<imagen>, aplica la operación del paso y manda el fragmento al paso siguiente."""

import json
import time

from comun import claves, colas, pixeles
from worker.operaciones import OPERACIONES


def ahora_en_ms():
    return int(time.time() * 1000)


def manejador(redis, nombre, ttl=claves.TTL_POR_DEFECTO, ahora_ms=ahora_en_ms):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada tarea; lo que escribe vence en `ttl` s."""

    def procesar(canal, cuerpo):
        recibida = ahora_ms()
        tarea = json.loads(cuerpo)
        job_id, archivo, indice, paso = tarea["job_id"], tarea["archivo"], tarea["fragmento"], tarea["paso"]
        operacion = tarea["operaciones"][paso]["operacion"]
        entrada = pixeles.leer(redis.get(claves.fragmento(job_id, archivo, indice, paso)))
        inicio = ahora_ms()
        salida = OPERACIONES[operacion](entrada)
        proc_ms = ahora_ms() - inicio
        redis.set(claves.fragmento(job_id, archivo, indice, paso + 1), pixeles.a_png(salida), ex=ttl)
        colas.publicar(canal, "", {"job_id": job_id, "archivo": archivo, "fragmento": indice, "worker": nombre,
                                   "ms": proc_ms, "intento": tarea["intento"]}, exchange=colas.RESULTADOS)
        clave_job = claves.job(job_id)
        with redis.pipeline() as transaccion:  # sumas y cantidades para los promedios de tiempos_s (contrato §3)
            transaccion.hincrby(clave_job, "cola_ms", recibida - tarea["encolado"])
            transaccion.hincrby(clave_job, "cola_n", 1)
            transaccion.hincrby(clave_job, "proc_ms", proc_ms)
            transaccion.hincrby(clave_job, "proc_n", 1)
            transaccion.sadd(claves.workers(job_id), nombre)
            transaccion.expire(claves.workers(job_id), ttl)
            transaccion.execute()
        # Se borra acá y no después del ack porque `consumir` hace el ack al volver del callback.
        redis.delete(claves.fragmento(job_id, archivo, indice, paso))

    return procesar
