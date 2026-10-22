"""Worker: toma cada tarea de sobel.tareas.<imagen>, aplica la operación del paso y manda el fragmento
al paso siguiente."""

import contextlib
import json
import logging
import os
import signal
import socket
import sys
import threading
import time

from comun import claves, colas, conexion, pixeles, registro, salud
from worker.operaciones import OPERACIONES

VIGENCIA_DEL_ANUNCIO = 15  # s: si el worker deja de anunciarse, deja de contar como activo (contrato §10)
CADA_CUANTO_ANUNCIAR = 5  # s
TIMEOUT_POR_DEFECTO = 60  # s por fragmento (SOBEL_TIMEOUT, contrato §12)


def ahora_en_ms():
    return int(time.time() * 1000)


class OperacionNoOfrecida(Exception):
    """La tarea pide una operación que este worker no ofrece (SOBEL_OPERACIONES): va a la DLQ."""


class TiempoAgotado(Exception):
    """La operación pasó SOBEL_TIMEOUT: la tarea va a la DLQ."""


@contextlib.contextmanager
def tiempo_limite(segundos):
    """Corta lo que corre adentro con TiempoAgotado si pasa `segundos`, con SIGALRM.

    Sólo en el hilo principal, que es donde pika corre el callback. Corta entre instrucciones de Python (entre
    las ventanas del Sobel), no en el medio de una operación de numpy. Al salir desarma el timer y deja el
    handler que había.
    """
    def vencer(_senal, _frame):
        raise TiempoAgotado(f"la operación pasó los {segundos} s")

    anterior = signal.signal(signal.SIGALRM, vencer)
    signal.setitimer(signal.ITIMER_REAL, segundos)
    try:
        yield
    finally:
        try:
            signal.setitimer(signal.ITIMER_REAL, 0)
        finally:  # si vence justo acá, la tarea va a la DLQ, pero el handler se restaura igual
            signal.signal(signal.SIGALRM, anterior)


def mandar_adelante(canal, tarea, nombre, proc_ms, ahora_ms):
    """Publica el paso siguiente de la tarea o, si era el último, el resultado. Devuelve a dónde la mandó."""
    siguiente = tarea["paso"] + 1
    if siguiente < len(tarea["operaciones"]):
        # Cada paso es una tarea nueva, con sus propios reintentos. Se declara antes de publicar por si
        # todavía no arrancó ningún worker de esa imagen (contrato §9).
        destino = colas.declarar_tareas(canal, tarea["operaciones"][siguiente]["imagen"])
        colas.publicar(canal, destino, {**tarea, "paso": siguiente, "intento": 1, "encolado": ahora_ms()})
        return destino
    colas.publicar(canal, "", {"job_id": tarea["job_id"], "archivo": tarea["archivo"], "fragmento": tarea["fragmento"],
                               "worker": nombre, "ms": proc_ms, "intento": tarea["intento"]}, exchange=colas.RESULTADOS)
    return colas.RESULTADOS


def manejador(redis, nombre, ofrecidas=tuple(OPERACIONES), funciones=OPERACIONES, timeout=TIMEOUT_POR_DEFECTO,
              ttl=claves.TTL_POR_DEFECTO, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.worker")):
    """Devuelve el callback(canal, cuerpo) que `consumir` llama por cada tarea; lo que escribe vence en `ttl` s.

    Una excepción (operación no ofrecida, fragmento ilegible, timeout) hace que `consumir` mande la tarea a la DLQ.
    """

    def procesar(canal, cuerpo):
        recibida = ahora_ms()
        tarea = json.loads(cuerpo)
        job_id, archivo, indice, paso = tarea["job_id"], tarea["archivo"], tarea["fragmento"], tarea["paso"]
        operacion = tarea["operaciones"][paso]["operacion"]
        clave_job = claves.job(job_id)
        clave_entrada = claves.fragmento(job_id, archivo, indice, paso)
        clave_salida = claves.fragmento(job_id, archivo, indice, paso + 1)
        if not redis.exists(clave_job):
            # Venció (24 h) o nunca terminó de crearse: nadie lo va a consultar, y sumarle tiempos lo
            # recrearía sin vencimiento.
            log.warning("job %s no existe, se descarta el fragmento %d del archivo %d", job_id, indice, archivo)
            return
        if operacion not in ofrecidas:
            raise OperacionNoOfrecida(f"este worker no ofrece {operacion} (ofrece: {', '.join(ofrecidas)})")
        datos = redis.get(clave_entrada)
        if datos is None and redis.exists(clave_salida):
            # Redelivery de una tarea que ya terminó: se cortó entre el borrado de la entrada y el ack. La salida
            # está escrita; sólo falta mandarla. Sin sumar tiempos: ya se contaron (y no se sabe cuánto tardó).
            destino = mandar_adelante(canal, tarea, nombre, 0, ahora_ms)
            log.info("job %s: archivo %d, fragmento %d ya estaba hecho, se manda de nuevo → %s",
                     job_id, archivo, indice, destino)
            return
        if datos is None:
            # Ni reintentarla ni la DLQ lo arreglan: el fragmento ya no existe (venció, o el joiner ya lo unió).
            log.warning("job %s: archivo %d, fragmento %d sin entrada ni salida del paso %d, se descarta",
                        job_id, archivo, indice, paso)
            return
        entrada = pixeles.leer(datos)
        inicio = ahora_ms()
        with tiempo_limite(timeout):  # sólo la operación: cortar una llamada a Redis o a RabbitMQ deja todo a medias
            salida = funciones[operacion](entrada)
        proc_ms = ahora_ms() - inicio
        redis.set(clave_salida, pixeles.a_png(salida), ex=ttl)
        destino = mandar_adelante(canal, tarea, nombre, proc_ms, ahora_ms)
        log.info("job %s: archivo %d, fragmento %d, %s en %d ms → %s",
                 job_id, archivo, indice, operacion, proc_ms, destino)
        with redis.pipeline() as transaccion:  # sumas y cantidades para los promedios de tiempos_s (contrato §3)
            transaccion.hincrby(clave_job, "cola_ms", recibida - tarea["encolado"])
            transaccion.hincrby(clave_job, "cola_n", 1)
            transaccion.hincrby(clave_job, "proc_ms", proc_ms)
            transaccion.hincrby(clave_job, "proc_n", 1)
            transaccion.sadd(claves.workers(job_id), nombre)
            transaccion.expire(claves.workers(job_id), ttl)
            transaccion.execute()
        # Se borra acá y no después del ack porque `consumir` hace el ack al volver del callback.
        redis.delete(clave_entrada)

    return procesar


def anunciar(redis, nombre, imagen, ofrecidas):
    """Anota al worker como activo por VIGENCIA_DEL_ANUNCIO s: el backend valida cada paso contra esto (contrato §2)."""
    with redis.pipeline() as transaccion:  # sin vencimiento figuraría activo para siempre si se corta entre los dos
        transaccion.hset(claves.worker(nombre), mapping={
            "imagen": imagen, "operaciones": ",".join(ofrecidas), "host": nombre})
        transaccion.expire(claves.worker(nombre), VIGENCIA_DEL_ANUNCIO)
        transaccion.execute()


def anunciar_siempre(redis, nombre, imagen, ofrecidas, log):
    """Renueva el anuncio cada CADA_CUANTO_ANUNCIAR s, en su propio hilo: una tarea larga no lo frena."""
    while True:
        try:
            anunciar(redis, nombre, imagen, ofrecidas)
        except conexion.ERRORES_DE_CONEXION as error:  # Redis caído: se reintenta en la próxima vuelta
            log.warning("no se pudo renovar el anuncio del worker (%s)", error)
        except Exception:
            # Cualquier otra cosa también se reintenta, pero con el traceback: sin anuncio el backend
            # contesta SIN_WORKERS aunque el worker siga consumiendo.
            log.exception("no se pudo renovar el anuncio del worker")
        time.sleep(CADA_CUANTO_ANUNCIAR)


def operaciones_de_entorno():
    """SOBEL_OPERACIONES (contrato §12). Una operación que el worker no conoce corta el arranque."""
    pedidas = os.environ.get("SOBEL_OPERACIONES", "sobel,grises,blur").split(",")
    ofrecidas = list(dict.fromkeys(op.strip() for op in pedidas if op.strip()))  # sin repetidas, en orden
    desconocidas = sorted(set(ofrecidas) - set(OPERACIONES))
    if not ofrecidas or desconocidas:
        raise RuntimeError(f"SOBEL_OPERACIONES tiene que nombrar operaciones de {sorted(OPERACIONES)} "
                           f"(llegó {os.environ.get('SOBEL_OPERACIONES')!r})")
    return ofrecidas


def main():
    log, _ = registro.configurar("worker")

    def apagar(senal, _frame):
        # Como en el split: sin handler, como PID 1 del contenedor ignoraría el SIGTERM. La tarea en curso queda
        # sin ack y RabbitMQ la reentrega; repetir un paso no rompe nada (contrato §9).
        log.info("%s: el worker termina; si había una tarea en curso, RabbitMQ la reentrega",
                 signal.Signals(senal).name)
        sys.exit(0)

    signal.signal(signal.SIGTERM, apagar)
    imagen = conexion.variable_obligatoria("SOBEL_IMAGEN")
    ofrecidas = operaciones_de_entorno()
    timeout = conexion.entero_de_entorno("SOBEL_TIMEOUT", TIMEOUT_POR_DEFECTO, 1)
    ttl = conexion.entero_de_entorno("SOBEL_TTL", claves.TTL_POR_DEFECTO, 1)
    puerto = conexion.entero_de_entorno("SOBEL_PUERTO", 8080, 1)
    nombre = socket.gethostname()  # el nombre del pod (o de la VM)
    redis = conexion.cliente_redis()
    # Antes de conectarse: mientras espera con backoff, la probe ve `caido` en vez de no tener respuesta.
    salud.servir("worker", puerto, conexion.estado, redis=redis)
    log.info("worker %s: /health en el puerto %d", nombre, puerto)
    conexion.con_backoff(redis.ping, "redis", log=log)
    threading.Thread(target=anunciar_siempre, args=(redis, nombre, imagen, ofrecidas, log), daemon=True).start()

    def al_conectar(canal):
        colas.declarar(canal)
        return colas.declarar_tareas(canal, imagen)

    log.info("worker %s consumiendo sobel.tareas.%s, ofrece %s", nombre, imagen, ",".join(ofrecidas))
    tareas = manejador(redis, nombre, ofrecidas, timeout=timeout, ttl=ttl, log=log)
    conexion.consumir(None, tareas, al_conectar=al_conectar, cliente_redis=redis, log=log)


if __name__ == "__main__":
    main()
