"""Split: toma cada job de sobel.split, parte sus archivos en fragmentos y publica una tarea por fragmento."""

import json
import logging
import os
import time

from comun import colas, conexion, imagen, registro


def ahora_en_ms():
    return int(time.time() * 1000)


def manejador(redis, partes, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.split")):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada job."""

    def procesar(canal, cuerpo):
        inicio = time.monotonic()
        job_id = json.loads(cuerpo)["job_id"]
        job = f"job:{job_id}"
        operaciones = json.loads(redis.hget(job, "operaciones"))
        tareas = []
        for archivo in range(int(redis.hget(job, "archivos"))):
            matriz = imagen.leer_rgb(redis.get(f"{job}:entrada:{archivo}"))
            fragmentos = imagen.dividir(matriz, partes)
            geometria = {"total": len(fragmentos)}
            for f, (franja, extra, filas) in enumerate(fragmentos):
                redis.set(f"{job}:frag:{archivo}:{f}:0", imagen.a_png(franja))
                geometria[f] = f"{extra},{filas}"
                tareas.append({"job_id": job_id, "archivo": archivo, "fragmento": f, "paso": 0,
                               "operaciones": operaciones, "intento": 1})
            redis.hset(f"{job}:geometria:{archivo}", mapping=geometria)

        cola = colas.declarar_tareas(canal, operaciones[0]["imagen"])
        for tarea in tareas:
            colas.publicar(canal, cola, {**tarea, "encolado": ahora_ms()})
        redis.hset(job, "estado", "processing")
        log.info("job %s: %d fragmentos a %s en %.2f s", job_id, len(tareas), cola, time.monotonic() - inicio)

    return procesar


def main():
    log, _ = registro.configurar("split")
    redis = conexion.conectar_redis(log=log)
    partes = int(os.environ.get("SOBEL_PARTES", "4"))
    log.info("split consumiendo %s, %d partes por archivo", colas.SPLIT, partes)
    conexion.consumir(colas.SPLIT, manejador(redis, partes, log=log), al_conectar=colas.declarar,
                      cliente_redis=redis, log=log)


if __name__ == "__main__":
    main()
