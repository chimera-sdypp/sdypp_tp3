"""Los jobs en Redis (contrato §10). El backend crea el job; los demás servicios lo van completando."""

import json
import math
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from comun import almacen as punteros
from comun import claves

ESTADOS = ("queued", "processing", "done", "failed", "dlq")
TERMINADOS_MAX = 1000
RESERVA_S = 900  # cuánto espera una reserva de POST /jobs/presigned a que el cliente suba y confirme


def ahora_ms():
    return int(time.time() * 1000)


def crear(redis, almacen, ttl, usuario, operaciones, contenidos):
    """Sube los archivos al bucket y guarda el job con un puntero a cada uno. Devuelve (job_id, creado_ms).

    Los archivos van primero: si el bucket no responde (NoDisponible), el job no llega a existir.
    """
    job_id, creado = str(uuid.uuid4()), ahora_ms()
    punteros.escribir_varios(redis, almacen, [(claves.entrada(job_id, a), datos) for a, datos in enumerate(contenidos)],
                             ttl)
    tuberia = redis.pipeline()
    _registrar(tuberia, job_id, creado, ttl, usuario, operaciones, len(contenidos))
    tuberia.execute()
    return job_id, creado


def _registrar(tuberia, job_id, creado, ttl, usuario, operaciones, archivos):
    """Anota en `tuberia` el hash del job en `queued` y lo suma a los índices de /jobs."""
    clave = claves.job(job_id)
    tuberia.hset(clave, mapping={
        "usuario": usuario, "estado": "queued", "creado": creado, "operaciones": json.dumps(operaciones),
        "archivos": archivos, "fragmentos_total": 0, "completos": 0, "en_dlq": 0, "reintentos": 0,
        "cola_ms": 0, "cola_n": 0, "proc_ms": 0, "proc_n": 0,
    })
    tuberia.expire(clave, ttl)
    for indice in ("jobs", f"jobs:{usuario}"):
        tuberia.zadd(indice, {job_id: creado})
        tuberia.expire(indice, ttl)


def reservar(redis, almacen, usuario, operaciones, tamanos, hilos, segundos=RESERVA_S):
    """Abre una sesión de subida al bucket por archivo y anota la reserva (contrato §2b). Devuelve (job_id, urls).

    Todavía no hay job: lo crea `confirmar`, cuando los archivos ya están en el bucket. Si nadie confirma, la
    reserva vence a los `segundos` y lo que se haya subido lo borra la regla de vencimiento del bucket.
    """
    job_id = str(uuid.uuid4())
    nombres = [punteros.nombre_de(claves.entrada(job_id, a)) for a in range(len(tamanos))]
    with ThreadPoolExecutor(max_workers=min(hilos, len(nombres))) as pool:
        urls = list(pool.map(almacen.iniciar_subida, nombres, tamanos))
    clave = claves.reserva(job_id)
    with redis.pipeline() as tuberia:
        tuberia.hset(clave, mapping={"usuario": usuario, "operaciones": json.dumps(operaciones),
                                     "tamanos": json.dumps(tamanos)})
        tuberia.expire(clave, segundos)
        tuberia.execute()
    return job_id, urls


def leer_reserva(redis, job_id):
    """La reserva como {usuario, operaciones, tamanos}, o None si no existe, venció o ya se confirmó."""
    datos = _texto(redis.hgetall(claves.reserva(job_id)))
    if not datos:
        return None
    return {"usuario": datos["usuario"], "operaciones": json.loads(datos["operaciones"]),
            "tamanos": json.loads(datos["tamanos"])}


def confirmar(redis, job_id, ttl, reserva):
    """Convierte la reserva en un job en `queued` que apunta a los archivos que subió el cliente.

    Devuelve el `creado` (ms), o None si la reserva ya no estaba. Va en una transacción que vigila la reserva:
    de dos confirmaciones simultáneas del mismo job, una crea el job y la otra recibe None (no se encola dos veces).
    """
    clave_reserva = claves.reserva(job_id)
    creado = ahora_ms()

    def escribir(tuberia):
        if not tuberia.exists(clave_reserva):
            return None
        tuberia.multi()
        tuberia.delete(clave_reserva)
        for archivo in range(len(reserva["tamanos"])):  # los objetos ya están: sólo faltan los punteros
            clave = claves.entrada(job_id, archivo)
            tuberia.set(clave, punteros.nombre_de(clave), ex=ttl)
        _registrar(tuberia, job_id, creado, ttl, reserva["usuario"], reserva["operaciones"], len(reserva["tamanos"]))
        return creado

    return redis.transaction(escribir, clave_reserva, value_from_callable=True)


def devolver_reserva(redis, job_id, reserva, segundos=RESERVA_S):
    """Deshace `confirmar` si no se pudo encolar: el job vuelve a ser una reserva y los archivos quedan en el
    bucket, así el cliente reintenta la confirmación sin volver a subirlos."""
    archivos = len(reserva["tamanos"])
    clave = claves.reserva(job_id)
    with redis.pipeline() as tuberia:
        tuberia.delete(claves.job(job_id), *[claves.entrada(job_id, a) for a in range(archivos)])
        tuberia.zrem("jobs", job_id)
        tuberia.zrem(f"jobs:{reserva['usuario']}", job_id)
        tuberia.hset(clave, mapping={"usuario": reserva["usuario"], "operaciones": json.dumps(reserva["operaciones"]),
                                     "tamanos": json.dumps(reserva["tamanos"])})
        tuberia.expire(clave, segundos)
        tuberia.execute()


def borrar(redis, almacen, job_id, usuario, archivos):
    """Deshace `crear` si no se pudo encolar."""
    punteros.borrar(redis, almacen, *[f"job:{job_id}:entrada:{a}" for a in range(archivos)])
    tuberia = redis.pipeline()
    tuberia.delete(f"job:{job_id}")
    tuberia.zrem("jobs", job_id)
    tuberia.zrem(f"jobs:{usuario}", job_id)
    tuberia.execute()


def leer(redis, job_id):
    """El hash del job como dict de strings, o None si no existe (o venció)."""
    return _texto(redis.hgetall(f"job:{job_id}")) or None


def vista(redis, job_id, job):
    """Lo que devuelve GET /jobs/{id} (contrato §3)."""
    total, completos = int(job["fragmentos_total"]), int(job["completos"])
    terminado = int(job["terminado"]) if job.get("terminado") else None
    archivos = int(job["archivos"])
    return {
        "job_id": job_id, "usuario": job["usuario"], "estado": job["estado"],
        "creado": iso(int(job["creado"])), "terminado": iso(terminado) if terminado else None,
        "archivos": archivos,
        "operaciones": json.loads(job["operaciones"]),
        "fragmentos": {"total": total, "completos": completos,
                       "porcentaje": round(100 * completos / total, 1) if total else 0.0,
                       "en_dlq": int(job["en_dlq"])},
        "tiempos_s": {
            "split": _segundos(job.get("t_split_ms")),
            "cola": _promedio(job["cola_ms"], job["cola_n"]),
            "procesamiento": _promedio(job["proc_ms"], job["proc_n"]),
            "join": _segundos(job.get("t_join_ms")),
            "total": round((terminado - int(job["creado"])) / 1000, 3) if terminado else None,
        },
        "workers": sorted(w.decode() for w in redis.smembers(f"job:{job_id}:workers")),
        "reintentos": int(job["reintentos"]),
        "resultados": [f"/jobs/{job_id}/resultado/{n}" for n in range(archivos)] if job["estado"] == "done" else [],
        "error": json.loads(job["error"]) if job.get("error") else None,
    }


def listar(redis, ttl, usuario=None, estado=None, limite=None):
    """Los jobs vigentes, del más nuevo al más viejo: [{job_id, usuario, estado, creado}]."""
    indice = f"jobs:{usuario}" if usuario else "jobs"
    redis.zremrangebyscore(indice, "-inf", ahora_ms() - ttl * 1000)
    ids = [i.decode() for i in redis.zrevrange(indice, 0, -1)]
    tuberia = redis.pipeline()
    for job_id in ids:
        tuberia.hmget(f"job:{job_id}", "usuario", "estado", "creado")
    salida = []
    for job_id, (dueno, su_estado, creado) in zip(ids, tuberia.execute()):
        if su_estado is None or (estado and su_estado.decode() != estado):
            continue  # venció, o no es el estado pedido
        salida.append({"job_id": job_id, "usuario": dueno.decode(), "estado": su_estado.decode(),
                       "creado": iso(int(creado))})
        if limite and len(salida) == limite:
            break
    return salida


def estadisticas(redis, ttl):
    """La parte de /stats que sale de Redis: jobs por estado, workers, latencia y tasa de error."""
    por_estado = Counter(job["estado"] for job in listar(redis, ttl))
    terminados = [linea.decode().split() for linea in redis.lrange("terminados", 0, TERMINADOS_MAX - 1)]
    latencias = sorted(int(ms) / 1000 for _, ms in terminados)
    errores = sum(1 for su_estado, _ in terminados if su_estado in ("failed", "dlq"))
    return {
        "jobs": {e: por_estado.get(e, 0) for e in ESTADOS},
        "workers_activos": len(workers(redis)),
        "latencia_s": {"p50": _percentil(latencias, 50), "p95": _percentil(latencias, 95),
                       "p99": _percentil(latencias, 99), "muestras": len(latencias)},
        "tasa_error": round(errores / len(terminados), 4) if terminados else 0.0,
    }


def workers(redis):
    """Los workers registrados ahora (sus claves vencen a los 15 s si dejan de renovarlas)."""
    activos = []
    for clave in redis.scan_iter(match="worker:*", count=100):
        datos = _texto(redis.hgetall(clave))
        if datos:
            activos.append({"id": clave.decode().split(":", 1)[1], "imagen": datos.get("imagen", ""),
                            "operaciones": {o for o in datos.get("operaciones", "").split(",") if o}})
    return activos


def ofertas(redis):
    """{imagen: operaciones que ofrece algún worker activo de esa imagen}."""
    resultado = {}
    for worker in workers(redis):
        resultado.setdefault(worker["imagen"], set()).update(worker["operaciones"])
    return resultado


def volver_a_procesar(redis, job_ids):
    """Después de sacar fragmentos de sobel.dlq.agotados: el job vuelve a processing."""
    for job_id, cantidad in Counter(job_ids).items():
        clave = f"job:{job_id}"
        if not redis.exists(clave):
            continue
        tuberia = redis.pipeline()
        tuberia.hincrby(clave, "en_dlq", -cantidad)
        if redis.hget(clave, "estado") == b"dlq":
            tuberia.hset(clave, "estado", "processing")
            tuberia.hdel(clave, "error", "terminado")
        tuberia.execute()


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _texto(hash_crudo):
    return {k.decode(): v.decode() for k, v in hash_crudo.items()}


def _segundos(ms):
    return round(int(ms) / 1000, 3) if ms not in (None, "") else None


def _promedio(suma, cantidad):
    return round(int(suma) / int(cantidad) / 1000, 3) if int(cantidad) else None


def _percentil(ordenados, p):
    """Percentil por rango más cercano; None sin muestras."""
    if not ordenados:
        return None
    return ordenados[max(math.ceil(p / 100 * len(ordenados)) - 1, 0)]
