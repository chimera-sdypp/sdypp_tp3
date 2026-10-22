"""Joiner: cuenta cada fragmento completo de sobel.joiner, une cada archivo cuando están todos y cierra el job
en done, en el orden del contrato (§10, que el joiner una cada archivo una sola vez)."""

import json
import logging
import signal
import sys
import time

from comun import claves, colas, conexion, pixeles, registro, salud


def ahora_en_ms():
    return int(time.time() * 1000)


class FragmentoFaltante(Exception):
    """El archivo tiene todos sus fragmentos en `listos` pero falta alguno del último paso: va a la DLQ."""


def contar(redis, job_id, archivo, indice, ttl):
    """Anota el fragmento en `listos` y suma 1 a `completos`, juntos y sólo si es nuevo (un duplicado no suma).

    Si el job venció no escribe nada y devuelve False: el HINCRBY de `completos` lo recrearía sin TTL. El WATCH
    sobre el job hace que no pueda vencer entre el chequeo y la escritura.
    """
    clave_listos, clave_job = claves.listos(job_id, archivo), claves.job(job_id)

    def escribir(transaccion):
        if not transaccion.exists(clave_job):
            return False
        nuevo = not transaccion.sismember(clave_listos, indice)
        transaccion.multi()
        if nuevo:
            transaccion.sadd(clave_listos, indice)
            transaccion.hincrby(clave_job, "completos", 1)
        transaccion.expire(clave_listos, ttl)
        return True

    return redis.transaction(escribir, clave_listos, clave_job, value_from_callable=True)


def unir_si_corresponde(redis, job_id, archivo, ttl, ahora_ms):
    """Si el archivo tiene todos sus fragmentos completos y no está en `unidos`, lo une y lo marca.

    Devuelve (fragmentos, ms que tardó) si lo unió, o None si todavía no correspondía.

    El orden hace segura una reentrega: hasta el SADD en `unidos`, los fragmentos del último paso siguen ahí y
    otra pasada vuelve a unir. Dos réplicas que lo ven completo a la vez escriben los mismos bytes.
    """
    geometria = pixeles.leer_geometria(redis.hgetall(claves.geometria(job_id, archivo)))
    if redis.scard(claves.listos(job_id, archivo)) < len(geometria) or redis.sismember(claves.unidos(job_id), archivo):
        return None
    pasos = len(json.loads(redis.hget(claves.job(job_id), "operaciones")))  # el último paso es el de la cadena entera
    fragmentos = [claves.fragmento(job_id, archivo, indice, pasos) for indice in range(len(geometria))]
    inicio = ahora_ms()
    pngs = redis.mget(fragmentos)
    faltantes = [indice for indice, datos in enumerate(pngs) if datos is None]
    if faltantes:
        if redis.sismember(claves.unidos(job_id), archivo):
            return None  # otra réplica lo unió y borró los fragmentos entre el chequeo de arriba y el MGET
        raise FragmentoFaltante(f"job {job_id}: al archivo {archivo} le faltan los fragmentos {faltantes} "
                                f"del paso {pasos}")
    imagen = pixeles.unir([pixeles.leer(datos) for datos in pngs], geometria)
    redis.set(claves.resultado(job_id, archivo), pixeles.a_png(imagen), ex=ttl)
    t_join_ms = ahora_ms() - inicio
    with redis.pipeline() as transaccion:
        transaccion.hincrby(claves.job(job_id), "t_join_ms", t_join_ms)
        transaccion.sadd(claves.unidos(job_id), archivo)
        transaccion.expire(claves.unidos(job_id), ttl)
        transaccion.execute()
    # Recién ahora: ya está en `unidos`, nadie los vuelve a necesitar. Si el joiner se corta justo antes, la
    # reentrega ve el archivo unido y no los borra: quedan hasta que vence el TTL, sin dañar el resultado.
    redis.delete(*fragmentos)
    return len(geometria), t_join_ms


def cerrar_si_corresponde(redis, job_id, ahora_ms):
    """Si están unidos todos los archivos, pasa el job a done con `terminado` y la entrada en `terminados`.

    Todo junto y una sola vez: el WATCH reintenta si el job cambia en el medio, y un job que ya está en done o en
    failed no se toca, así otra réplica o una reentrega no lo cuentan dos veces en /stats. Devuelve si lo cerró.
    """
    clave_job = claves.job(job_id)
    if redis.scard(claves.unidos(job_id)) < int(redis.hget(clave_job, "archivos")):
        return False

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

    return redis.transaction(escribir, clave_job, value_from_callable=True)


def manejador(redis, ttl=claves.TTL_POR_DEFECTO, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.joiner")):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada fragmento completo de sobel.joiner."""

    def procesar(canal, cuerpo):
        mensaje = json.loads(cuerpo)
        job_id, archivo = mensaje["job_id"], mensaje["archivo"]
        if not contar(redis, job_id, archivo, mensaje["fragmento"], ttl):
            # Ni reintentarlo ni la DLQ lo arreglan: el job venció.
            log.warning("job %s no existe, se descarta el fragmento %d del archivo %d", job_id, mensaje["fragmento"],
                        archivo)
            return
        unido = unir_si_corresponde(redis, job_id, archivo, ttl, ahora_ms)
        if unido:
            log.info("job %s: archivo %d unido (%d fragmentos) en %d ms", job_id, archivo, *unido)
        if cerrar_si_corresponde(redis, job_id, ahora_ms):
            log.info("job %s: done", job_id)

    return procesar


def main():
    log, _ = registro.configurar("joiner")

    def apagar(senal, _frame):
        # Como en el split y el worker: sin handler, como PID 1 del contenedor ignoraría el SIGTERM. El fragmento en
        # curso queda sin ack y RabbitMQ lo reentrega; cada paso del manejador la soporta (contrato §10).
        log.info("%s: el joiner termina; si había un fragmento en curso, RabbitMQ lo reentrega",
                 signal.Signals(senal).name)
        sys.exit(0)

    signal.signal(signal.SIGTERM, apagar)
    ttl = conexion.entero_de_entorno("SOBEL_TTL", claves.TTL_POR_DEFECTO, 1)
    puerto = conexion.entero_de_entorno("SOBEL_PUERTO", 8080, 1)
    redis = conexion.cliente_redis()
    # Antes de conectarse: mientras espera con backoff, la probe ve `caido` en vez de no tener respuesta.
    salud.servir("joiner", puerto, conexion.estado, redis=redis)
    log.info("joiner: /health en el puerto %d", puerto)
    conexion.con_backoff(redis.ping, "redis", log=log)
    log.info("joiner consumiendo %s", colas.JOINER)
    conexion.consumir(colas.JOINER, manejador(redis, ttl, log=log), al_conectar=colas.declarar,
                      cliente_redis=redis, log=log)


if __name__ == "__main__":
    main()
