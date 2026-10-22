"""Joiner: cuenta cada fragmento completo de sobel.joiner, une cada archivo cuando están todos y cierra el job
en done."""

import json
import logging
import time

from comun import claves, pixeles


def ahora_en_ms():
    return int(time.time() * 1000)


def contar(redis, job_id, archivo, indice, ttl):
    """Anota el fragmento en `listos` y suma 1 a `completos`, juntos y sólo si es nuevo (un duplicado no suma)."""
    clave_listos = claves.listos(job_id, archivo)

    def escribir(transaccion):
        nuevo = not transaccion.sismember(clave_listos, indice)
        transaccion.multi()
        if nuevo:
            transaccion.sadd(clave_listos, indice)
            transaccion.hincrby(claves.job(job_id), "completos", 1)
        transaccion.expire(clave_listos, ttl)

    redis.transaction(escribir, clave_listos)


def unir_si_corresponde(redis, job_id, archivo, pasos, ttl, ahora_ms, log):
    """Si el archivo tiene todos sus fragmentos completos y no está en `unidos`, lo une y lo marca.

    El orden hace segura una reentrega: hasta el SADD en `unidos`, los fragmentos del último paso siguen ahí y
    otra pasada vuelve a unir. Dos réplicas que lo ven completo a la vez escriben los mismos bytes.
    """
    geometria = pixeles.leer_geometria(redis.hgetall(claves.geometria(job_id, archivo)))
    if redis.scard(claves.listos(job_id, archivo)) < len(geometria) or redis.sismember(claves.unidos(job_id), archivo):
        return
    fragmentos = [claves.fragmento(job_id, archivo, indice, pasos) for indice in range(len(geometria))]
    inicio = ahora_ms()
    imagen = pixeles.unir([pixeles.leer(datos) for datos in redis.mget(fragmentos)], geometria)
    redis.set(claves.resultado(job_id, archivo), pixeles.a_png(imagen), ex=ttl)
    t_join_ms = ahora_ms() - inicio
    with redis.pipeline() as transaccion:
        transaccion.hincrby(claves.job(job_id), "t_join_ms", t_join_ms)
        transaccion.sadd(claves.unidos(job_id), archivo)
        transaccion.expire(claves.unidos(job_id), ttl)
        transaccion.execute()
    redis.delete(*fragmentos)  # recién ahora: ya está en `unidos`, nadie los vuelve a necesitar
    log.info("job %s: archivo %d unido (%d fragmentos) en %d ms", job_id, archivo, len(geometria), t_join_ms)


def cerrar_si_corresponde(redis, job_id, ahora_ms, log):
    """Si están unidos todos los archivos, pasa el job a done con `terminado` y la entrada en `terminados`.

    Todo junto y una sola vez: el WATCH reintenta si el job cambia en el medio, y un job que ya está en done o en
    failed no se toca, así otra réplica o una reentrega no lo cuentan dos veces en /stats.
    """
    clave_job = claves.job(job_id)
    if redis.scard(claves.unidos(job_id)) < int(redis.hget(clave_job, "archivos")):
        return

    def escribir(transaccion):
        estado, creado = transaccion.hmget(clave_job, "estado", "creado")
        if estado in (b"done", b"failed"):
            return False
        terminado = ahora_ms()
        transaccion.multi()
        transaccion.hset(clave_job, mapping={"estado": "done", "terminado": terminado})
        transaccion.lpush(claves.TERMINADOS, f"done {terminado - int(creado)}")
        transaccion.ltrim(claves.TERMINADOS, 0, 999)
        return True

    if redis.transaction(escribir, clave_job, value_from_callable=True):
        log.info("job %s: done", job_id)


def manejador(redis, ttl=claves.TTL_POR_DEFECTO, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.joiner")):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada fragmento completo de sobel.joiner."""

    def procesar(canal, cuerpo):
        mensaje = json.loads(cuerpo)
        job_id, archivo = mensaje["job_id"], mensaje["archivo"]
        contar(redis, job_id, archivo, mensaje["fragmento"], ttl)
        pasos = len(json.loads(redis.hget(claves.job(job_id), "operaciones")))
        unir_si_corresponde(redis, job_id, archivo, pasos, ttl, ahora_ms, log)
        cerrar_si_corresponde(redis, job_id, ahora_ms, log)

    return procesar
