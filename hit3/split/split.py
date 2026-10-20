"""Split: toma cada job de sobel.split, parte sus archivos en fragmentos y publica una tarea por fragmento."""

import json
import logging
import os
import time

from comun import colas, conexion, pixeles, registro


# Suman una fila de borde por lado (contrato §10). Supone kernels 3×3: el de `blur` se fija en M3 y se
# confirma con Tomás; si fuera más grande, haría falta más de una fila por `blur`.
OPERACIONES_DE_VECINDAD = {"sobel", "blur"}


def ahora_en_ms():
    return int(time.time() * 1000)


def fallar_por_archivo_invalido(redis, job, mensaje, ahora):
    """Deja el job en `failed` con error ARCHIVO_INVALIDO y lo anota en `terminados` (los últimos 1000)."""
    error = {"tipo": "ARCHIVO_INVALIDO", "mensaje": mensaje}
    redis.hset(job, mapping={"estado": "failed", "error": json.dumps(error)})
    redis.lpush("terminados", f"failed {ahora - int(redis.hget(job, 'creado'))}")
    redis.ltrim("terminados", 0, 999)


def manejador(redis, partes, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.split")):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada job."""

    def procesar(canal, cuerpo):
        inicio = ahora_ms()
        job_id = json.loads(cuerpo)["job_id"]
        job = f"job:{job_id}"
        operaciones = json.loads(redis.hget(job, "operaciones"))
        filas_de_borde = sum(paso["operacion"] in OPERACIONES_DE_VECINDAD for paso in operaciones)
        tareas = []
        for archivo in range(int(redis.hget(job, "archivos"))):
            try:
                matriz = pixeles.leer_rgb(redis.get(f"{job}:entrada:{archivo}"))
            except ValueError:
                # Lo ya escrito vence por TTL; no se publica nada, así `failed` queda final (contrato §3).
                mensaje = f"el archivo {archivo} no se pudo decodificar"
                fallar_por_archivo_invalido(redis, job, mensaje, ahora_ms())
                log.warning("job %s: %s, queda en failed", job_id, mensaje)
                return
            fragmentos = pixeles.dividir(matriz, partes, filas_de_borde)
            geometria = {"total": len(fragmentos)}
            for f, (franja, extra, filas) in enumerate(fragmentos):
                redis.set(f"{job}:frag:{archivo}:{f}:0", pixeles.a_png(franja))
                geometria[f] = f"{extra},{filas}"
                tareas.append({"job_id": job_id, "archivo": archivo, "fragmento": f, "paso": 0,
                               "operaciones": operaciones, "intento": 1})
            redis.hset(f"{job}:geometria:{archivo}", mapping=geometria)

        cola = colas.declarar_tareas(canal, operaciones[0]["imagen"])
        for tarea in tareas:
            colas.publicar(canal, cola, {**tarea, "encolado": ahora_ms()})
        t_split_ms = ahora_ms() - inicio
        redis.hset(job, mapping={"estado": "processing", "fragmentos_total": len(tareas),
                                 "t_split_ms": t_split_ms})
        log.info("job %s: %d fragmentos a %s en %d ms", job_id, len(tareas), cola, t_split_ms)

    return procesar


def main():
    log, _ = registro.configurar("split")
    redis = conexion.conectar_redis(log=log)
    partes = int(os.environ.get("SOBEL_PARTES", "4"))
    if partes < 1:
        raise RuntimeError(f"SOBEL_PARTES tiene que ser al menos 1 (llegó {partes})")
    log.info("split consumiendo %s, %d partes por archivo", colas.SPLIT, partes)
    conexion.consumir(colas.SPLIT, manejador(redis, partes, log=log), al_conectar=colas.declarar,
                      cliente_redis=redis, log=log)


if __name__ == "__main__":
    main()
