"""Split: toma cada job de sobel.split, parte sus archivos en fragmentos y publica una tarea por fragmento."""

import json
import logging
import signal
import sys
import time

from comun import claves, colas, conexion, metricas, pixeles, registro, salud


# Suman una fila de borde por lado (contrato §10): las dos son 3×3.
OPERACIONES_DE_VECINDAD = {"sobel", "blur"}


def ahora_en_ms():
    return int(time.time() * 1000)


def claves_de_entrada(job_id, archivos):
    return [claves.entrada(job_id, archivo) for archivo in range(archivos)]


def fallar_por_archivo_invalido(redis, job_id, archivos, mensaje, ahora):
    """Deja el job en `failed` con error ARCHIVO_INVALIDO y `terminado`, lo anota en `terminados` (los últimos 1000)
    y borra las entradas. Todo junto: si se cortara en el medio, el redelivery vería `failed` y no completaría el
    resto."""
    clave_job = claves.job(job_id)
    error = {"tipo": "ARCHIVO_INVALIDO", "mensaje": mensaje}
    total_ms = ahora - int(redis.hget(clave_job, "creado"))
    with redis.pipeline() as transaccion:
        transaccion.hset(clave_job, mapping={"estado": "failed", "error": json.dumps(error), "terminado": ahora})
        transaccion.lpush(claves.TERMINADOS, f"failed {total_ms}")
        transaccion.ltrim(claves.TERMINADOS, 0, 999)
        transaccion.delete(*claves_de_entrada(job_id, archivos))
        transaccion.execute()


def pasar_a_processing(redis, job_id, t_split_ms):
    """Anota `t_split_ms` y pasa el job a `processing` sólo si sigue en `queued`.

    Las tareas ya están publicadas: con un job chico, workers y joiner pueden haberlo puesto en `done`
    antes de esta escritura, y `done` es final (contrato §3). El WATCH reintenta si cambia en el medio.
    """
    clave_job = claves.job(job_id)

    def escribir(transaccion):
        en_cola = transaccion.hget(clave_job, "estado") == b"queued"
        transaccion.multi()
        transaccion.hset(clave_job, "t_split_ms", t_split_ms)
        if en_cola:
            transaccion.hset(clave_job, "estado", "processing")

    redis.transaction(escribir, clave_job)


def manejador(redis, partes, ttl=claves.TTL_POR_DEFECTO, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.split")):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada job; lo que escribe vence en `ttl` s."""

    def procesar(canal, cuerpo):
        with metricas.fallos("split", "split"):  # lo que lance va a la DLQ y cuenta como error (contrato §11)
            repartir(canal, cuerpo)

    def repartir(canal, cuerpo):
        inicio = ahora_ms()
        job_id = json.loads(cuerpo)["job_id"]
        clave_job = claves.job(job_id)
        estado = redis.hget(clave_job, "estado")
        if estado is None:
            # Ni reintentarlo ni la DLQ lo arreglan: el job ya no está, y el reasignador no tendría qué marcar.
            log.warning("job %s no existe (venció o el backend no lo terminó de crear), se descarta", job_id)
            return
        if estado != b"queued":
            # Redelivery: ya se repartió (o falló) antes del ack. Repetir mandaría las tareas dos veces.
            log.info("job %s ya está en %s, no se reparte de nuevo", job_id, estado.decode())
            return
        operaciones = json.loads(redis.hget(clave_job, "operaciones"))
        filas_de_borde = sum(paso["operacion"] in OPERACIONES_DE_VECINDAD for paso in operaciones)
        tareas = []
        archivos = int(redis.hget(clave_job, "archivos"))
        for archivo in range(archivos):
            try:
                matriz = pixeles.leer_rgb(redis.get(claves.entrada(job_id, archivo)))
            except ValueError:
                # Lo ya escrito vence por TTL; no se publica nada, así `failed` queda final (contrato §3).
                mensaje = f"el archivo {archivo} no se pudo decodificar"
                fallar_por_archivo_invalido(redis, job_id, archivos, mensaje, ahora_ms())
                log.warning("job %s: %s, queda en failed", job_id, mensaje)
                metricas.fallo("split", "split")
                metricas.ERRORES.labels("split", "ARCHIVO_INVALIDO").inc()
                return
            fragmentos = pixeles.dividir(matriz, partes, filas_de_borde)
            for f, (franja, _, _) in enumerate(fragmentos):
                redis.set(claves.fragmento(job_id, archivo, f, 0), pixeles.a_png(franja), ex=ttl)
                tareas.append({"job_id": job_id, "archivo": archivo, "fragmento": f, "paso": 0,
                               "operaciones": operaciones, "intento": 1})
            clave_geometria = claves.geometria(job_id, archivo)
            with redis.pipeline() as transaccion:  # sin TTL quedaría para siempre si se corta entre los dos
                transaccion.hset(clave_geometria, mapping=pixeles.hash_de_geometria(fragmentos))
                transaccion.expire(clave_geometria, ttl)
                transaccion.execute()

        redis.hset(clave_job, "fragmentos_total", len(tareas))
        cola = colas.declarar_tareas(canal, operaciones[0]["imagen"])
        for tarea in tareas:
            colas.publicar(canal, cola, {**tarea, "encolado": ahora_ms()})
        t_split_ms = ahora_ms() - inicio
        pasar_a_processing(redis, job_id, t_split_ms)
        # Recién ahora: fuera de queued, un redelivery ya no las necesita (contrato §10: las borra el split).
        redis.delete(*claves_de_entrada(job_id, archivos))
        log.info("job %s: %d fragmentos a %s en %d ms", job_id, len(tareas), cola, t_split_ms)
        metricas.trabajo("split", "split", t_split_ms / 1000)

    return procesar


def main():
    log, _ = registro.configurar("split")

    def apagar(senal, _frame):
        # Como PID 1 del contenedor, sin handler el SIGTERM se ignora y k8s espera el período de gracia
        # para matarlo. Salir corta el job en curso sin ack y RabbitMQ lo reentrega. Si la señal llega
        # mientras se publican las tareas, el redelivery repite las ya publicadas (contrato §9: no rompe nada).
        log.info("%s: el split termina; si había un job en curso, RabbitMQ lo reentrega",
                 signal.Signals(senal).name)
        sys.exit(0)

    signal.signal(signal.SIGTERM, apagar)
    partes = conexion.entero_de_entorno("SOBEL_PARTES", 4, 1)
    ttl = conexion.entero_de_entorno("SOBEL_TTL", claves.TTL_POR_DEFECTO, 1)
    puerto = conexion.entero_de_entorno("SOBEL_PUERTO", 8080, 1)
    redis = conexion.cliente_redis()
    # Antes de conectarse: mientras espera con backoff, la probe ve `caido` en vez de no tener respuesta.
    salud.servir("split", puerto, conexion.estado, redis=redis)
    log.info("split: /health en el puerto %d", puerto)
    conexion.con_backoff(redis.ping, "redis", log=log)
    log.info("split consumiendo %s, %d partes por archivo", colas.SPLIT, partes)
    conexion.consumir(colas.SPLIT, manejador(redis, partes, ttl, log=log), al_conectar=colas.declarar,
                      cliente_redis=redis, servicio="split", log=log)


if __name__ == "__main__":
    main()
