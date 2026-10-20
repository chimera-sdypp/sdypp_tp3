"""Split: toma cada job de sobel.split, parte sus archivos en fragmentos y publica una tarea por fragmento."""

import json
import logging
import os
import time

from comun import colas, conexion, pixeles, registro


# Suman una fila de borde por lado (contrato §10). Supone kernels 3×3: el de `blur` se fija en M3 y se
# confirma con Tomás; si fuera más grande, haría falta más de una fila por `blur`.
OPERACIONES_DE_VECINDAD = {"sobel", "blur"}


TTL_POR_DEFECTO = 86400  # SOBEL_TTL: las claves de un job viven 24 h (contrato §10)


def ahora_en_ms():
    return int(time.time() * 1000)


def fallar_por_archivo_invalido(redis, job, archivos, mensaje, ahora):
    """Deja el job en `failed` con error ARCHIVO_INVALIDO, lo anota en `terminados` (los últimos 1000) y borra
    las entradas. Todo junto: si se cortara en el medio, el redelivery vería `failed` y no completaría el resto."""
    error = {"tipo": "ARCHIVO_INVALIDO", "mensaje": mensaje}
    total_ms = ahora - int(redis.hget(job, "creado"))
    with redis.pipeline() as transaccion:
        transaccion.hset(job, mapping={"estado": "failed", "error": json.dumps(error)})
        transaccion.lpush("terminados", f"failed {total_ms}")
        transaccion.ltrim("terminados", 0, 999)
        transaccion.delete(*(f"{job}:entrada:{archivo}" for archivo in range(archivos)))
        transaccion.execute()


def pasar_a_processing(redis, job, t_split_ms):
    """Anota `t_split_ms` y pasa el job a `processing` sólo si sigue en `queued`.

    Las tareas ya están publicadas: con un job chico, workers y joiner pueden haberlo puesto en `done`
    antes de esta escritura, y `done` es final (contrato §3). El WATCH reintenta si cambia en el medio.
    """
    def escribir(transaccion):
        en_cola = transaccion.hget(job, "estado") == b"queued"
        transaccion.multi()
        transaccion.hset(job, "t_split_ms", t_split_ms)
        if en_cola:
            transaccion.hset(job, "estado", "processing")

    redis.transaction(escribir, job)


def manejador(redis, partes, ttl=TTL_POR_DEFECTO, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.split")):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada job; lo que escribe vence en `ttl` s."""

    def procesar(canal, cuerpo):
        inicio = ahora_ms()
        job_id = json.loads(cuerpo)["job_id"]
        job = f"job:{job_id}"
        estado = redis.hget(job, "estado")
        if estado is None:
            # Ni reintentarlo ni la DLQ lo arreglan: sobel.split no tiene DLX y el reasignador maneja tareas.
            log.warning("job %s no existe (venció o el backend no lo terminó de crear), se descarta", job_id)
            return
        if estado != b"queued":
            # Redelivery: ya se repartió (o falló) antes del ack. Repetir mandaría las tareas dos veces.
            log.info("job %s ya está en %s, no se reparte de nuevo", job_id, estado.decode())
            return
        operaciones = json.loads(redis.hget(job, "operaciones"))
        filas_de_borde = sum(paso["operacion"] in OPERACIONES_DE_VECINDAD for paso in operaciones)
        tareas = []
        archivos = int(redis.hget(job, "archivos"))
        for archivo in range(archivos):
            try:
                matriz = pixeles.leer_rgb(redis.get(f"{job}:entrada:{archivo}"))
            except ValueError:
                # Lo ya escrito vence por TTL; no se publica nada, así `failed` queda final (contrato §3).
                mensaje = f"el archivo {archivo} no se pudo decodificar"
                fallar_por_archivo_invalido(redis, job, archivos, mensaje, ahora_ms())
                log.warning("job %s: %s, queda en failed", job_id, mensaje)
                return
            fragmentos = pixeles.dividir(matriz, partes, filas_de_borde)
            geometria = {"total": len(fragmentos)}
            for f, (franja, extra, filas) in enumerate(fragmentos):
                redis.set(f"{job}:frag:{archivo}:{f}:0", pixeles.a_png(franja), ex=ttl)
                geometria[f] = f"{extra},{filas}"
                tareas.append({"job_id": job_id, "archivo": archivo, "fragmento": f, "paso": 0,
                               "operaciones": operaciones, "intento": 1})
            with redis.pipeline() as transaccion:  # sin TTL quedaría para siempre si se corta entre los dos
                transaccion.hset(f"{job}:geometria:{archivo}", mapping=geometria)
                transaccion.expire(f"{job}:geometria:{archivo}", ttl)
                transaccion.execute()

        redis.hset(job, "fragmentos_total", len(tareas))
        cola = colas.declarar_tareas(canal, operaciones[0]["imagen"])
        for tarea in tareas:
            colas.publicar(canal, cola, {**tarea, "encolado": ahora_ms()})
        t_split_ms = ahora_ms() - inicio
        pasar_a_processing(redis, job, t_split_ms)
        # Recién ahora: fuera de queued, un redelivery ya no las necesita (contrato §10: las borra el split).
        redis.delete(*(f"{job}:entrada:{archivo}" for archivo in range(archivos)))
        log.info("job %s: %d fragmentos a %s en %d ms", job_id, len(tareas), cola, t_split_ms)

    return procesar


def main():
    log, _ = registro.configurar("split")
    redis = conexion.conectar_redis(log=log)
    partes = int(os.environ.get("SOBEL_PARTES", "4"))
    if partes < 1:
        raise RuntimeError(f"SOBEL_PARTES tiene que ser al menos 1 (llegó {partes})")
    ttl = int(os.environ.get("SOBEL_TTL", TTL_POR_DEFECTO))
    if ttl < 1:
        raise RuntimeError(f"SOBEL_TTL tiene que ser al menos 1 segundo (llegó {ttl})")
    log.info("split consumiendo %s, %d partes por archivo", colas.SPLIT, partes)
    conexion.consumir(colas.SPLIT, manejador(redis, partes, ttl, log=log), al_conectar=colas.declarar,
                      cliente_redis=redis, log=log)


if __name__ == "__main__":
    main()
