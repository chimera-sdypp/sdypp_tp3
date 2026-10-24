"""Reasignador: consume sobel.dlq. Vuelve a encolar cada tarea que falló, con un intento más; la que agotó los
reintentos va a sobel.dlq.agotados y su job a `dlq`; un job del split o un fragmento completo del joiner pasan el job
a `failed` (contrato §9 y §10)."""

import json
import logging
import signal
import sys
import time

from comun import claves, colas, conexion, metricas, registro, salud

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


def agotar(canal, redis, tarea, motivo, ahora_ms):
    """Manda la tarea tal cual a sobel.dlq.agotados (el backend le pone intento 1 al reprocesarla) y pasa el job a
    `dlq` (contrato §10).

    Publica primero, por lo mismo que `reintentar`. Si se cae antes del ack, la reentrega publica otra copia: después
    de la transacción, `en_dlq` también suma dos veces y la cuenta cierra al reprocesar; antes de la transacción,
    `en_dlq` queda en 1 con dos copias y el backend, al reprocesar, lo deja en -1. Perder la tarea sería peor.
    """
    colas.publicar(canal, colas.AGOTADOS, tarea)
    operacion = tarea["operaciones"][tarea["paso"]]["operacion"]
    explicacion = (f"la tarea de {operacion} del fragmento {tarea['fragmento']} del archivo {tarea['archivo']} agotó "
                   f"los reintentos ({motivo})")
    pasar_a(redis, tarea["job_id"], "dlq", {"tipo": "REINTENTOS_AGOTADOS", "mensaje": explicacion}, ahora_ms,
            suma_en_dlq=True)


def pasar_a(redis, job_id, destino, error, ahora_ms, suma_en_dlq=False):
    """Pasa el job a `destino` (`dlq` o `failed`) con `error`, `terminado` y la entrada en `terminados`, todo junto,
    y con `suma_en_dlq` suma 1 a `en_dlq`. Una sola vez: si ya está en `destino`, sólo suma. `done` y `failed` son
    finales (contrato §3) y no cambian, pero `en_dlq` suma igual: cuenta lo que quedó en sobel.dlq.agotados, y el
    backend lo resta al reprocesar sin cambiarle el estado. El WATCH reintenta si el job cambia en el medio.

    Devuelve el estado en que quedó el job, o None si venció.
    """
    clave_job = claves.job(job_id)

    def escribir(transaccion):
        actual, creado = transaccion.hmget(clave_job, "estado", "creado")
        if actual is None:  # venció: un HINCRBY o un HSET lo recrearían sin TTL
            return None
        cambia = actual not in (b"done", b"failed", destino.encode())
        transaccion.multi()
        if suma_en_dlq:
            transaccion.hincrby(clave_job, "en_dlq", 1)
        if cambia:
            terminado = ahora_ms()
            transaccion.hset(clave_job, mapping={"estado": destino, "error": json.dumps(error), "terminado": terminado})
            transaccion.lpush(claves.TERMINADOS, f"{destino} {terminado - int(creado)}")
            transaccion.ltrim(claves.TERMINADOS, 0, 999)
        return destino if cambia else actual.decode()

    return redis.transaction(escribir, clave_job, value_from_callable=True)


def manejador(redis, reintentos_max, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.reasignador")):
    """Devuelve el callback(canal, cuerpo, propiedades) que `consumir` llama por cada mensaje de sobel.dlq."""

    def procesar(canal, cuerpo, propiedades):
        try:
            origen, mensaje, motivo = leer(cuerpo, propiedades)
        except Ilegible as problema:
            # sobel.dlq no tiene DLX: un nack lo descartaría igual, así que se descarta con el detalle en el log.
            log.error("mensaje ilegible en %s, se descarta: %s", colas.DLQ, problema)
            metricas.ERRORES.labels("reasignador", "MENSAJE_ILEGIBLE").inc()
            return
        job_id = mensaje["job_id"]
        estado_actual = redis.hget(claves.job(job_id), "estado")
        if estado_actual is None:
            # Ni reintentarlo ni marcar el job lo arreglan: venció.
            log.warning("job %s no existe, se descarta un mensaje de %s", job_id, origen)
            return
        if origen == TAREA:
            if estado_actual in (b"done", b"failed"):
                # Reintentarla ya no sirve: el job terminó (por ejemplo, el joiner lo pasó a failed).
                log.warning("job %s ya está en %s, se descarta una tarea del archivo %d", job_id,
                            estado_actual.decode(), mensaje["archivo"])
            elif mensaje["intento"] <= reintentos_max:
                inicio = ahora_ms()
                with metricas.fallos("reasignador", "reasignar"):
                    intento = reintentar(canal, redis, mensaje, ahora_ms)
                metricas.trabajo("reasignador", "reasignar", (ahora_ms() - inicio) / 1000)
                log.info("job %s: archivo %d, fragmento %d, paso %d (%s): intento %d", job_id, mensaje["archivo"],
                         mensaje["fragmento"], mensaje["paso"], motivo, intento)
            else:
                inicio = ahora_ms()
                with metricas.fallos("reasignador", "agotar"):
                    agotar(canal, redis, mensaje, motivo, ahora_ms)
                metricas.trabajo("reasignador", "agotar", (ahora_ms() - inicio) / 1000)
                log.warning("job %s: el fragmento %d del archivo %d agotó los reintentos en el paso %d (%s), va a %s",
                            job_id, mensaje["fragmento"], mensaje["archivo"], mensaje["paso"], motivo, colas.AGOTADOS)
        else:
            # Un job del split o un fragmento completo del joiner no se arreglan reintentando (contrato §9).
            servicio = "split" if origen == colas.SPLIT else "joiner"
            error_del_job = {"tipo": "ERROR_INTERNO", "mensaje": f"falló en el {servicio} ({motivo})"}
            quedo = pasar_a(redis, job_id, "failed", error_del_job, ahora_ms)
            # Pasa a failed, o ya estaba en failed o en done (otro servicio lo terminó), o venció en el medio.
            log.warning("job %s: falló en el %s (%s), queda en %s", job_id, servicio, motivo, quedo or "nada: venció")

    return procesar


def main():
    log, _ = registro.configurar("reasignador")

    def apagar(senal, _frame):
        # Como en el split, el worker y el joiner: sin handler, como PID 1 del contenedor ignoraría el SIGTERM. El
        # mensaje en curso queda sin ack y RabbitMQ lo reentrega: a lo sumo queda una tarea duplicada, que el worker
        # soporta (contrato §9, punto 4), o un contador informativo de más.
        log.info("%s: el reasignador termina; si había un mensaje en curso, RabbitMQ lo reentrega",
                 signal.Signals(senal).name)
        sys.exit(0)

    signal.signal(signal.SIGTERM, apagar)
    reintentos_max = conexion.entero_de_entorno("SOBEL_REINTENTOS_MAX", 3, 0)
    puerto = conexion.entero_de_entorno("SOBEL_PUERTO", 8080, 1)
    redis = conexion.cliente_redis()
    # Antes de conectarse: mientras espera con backoff, la probe ve `caido` en vez de no tener respuesta.
    salud.servir("reasignador", puerto, conexion.estado, redis=redis)
    log.info("reasignador: /health en el puerto %d", puerto)
    conexion.con_backoff(redis.ping, "redis", log=log)
    log.info("reasignador consumiendo %s, con %d reintentos por tarea", colas.DLQ, reintentos_max)
    conexion.consumir(colas.DLQ, manejador(redis, reintentos_max, log=log), al_conectar=colas.declarar,
                      cliente_redis=redis, con_propiedades=True, servicio="reasignador", log=log)


if __name__ == "__main__":
    main()
