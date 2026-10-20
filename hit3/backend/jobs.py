"""Los jobs en Redis (contrato §10). El backend crea el job; los demás servicios lo van completando."""

import json
import math
import time
import uuid
from collections import Counter
from datetime import UTC, datetime

ESTADOS = ("queued", "processing", "done", "failed", "dlq")
TERMINADOS_MAX = 1000


def ahora_ms():
    return int(time.time() * 1000)


def crear(redis, ttl, usuario, operaciones, contenidos):
    """Guarda el job y sus archivos. Devuelve (job_id, creado_ms)."""
    job_id, creado = str(uuid.uuid4()), ahora_ms()
    clave = f"job:{job_id}"
    tuberia = redis.pipeline()
    tuberia.hset(clave, mapping={
        "usuario": usuario, "estado": "queued", "creado": creado, "operaciones": json.dumps(operaciones),
        "archivos": len(contenidos), "fragmentos_total": 0, "completos": 0, "en_dlq": 0, "reintentos": 0,
        "cola_ms": 0, "cola_n": 0, "proc_ms": 0, "proc_n": 0,
    })
    tuberia.expire(clave, ttl)
    for archivo, datos in enumerate(contenidos):
        tuberia.set(f"{clave}:entrada:{archivo}", datos, ex=ttl)
    for indice in ("jobs", f"jobs:{usuario}"):
        tuberia.zadd(indice, {job_id: creado})
        tuberia.expire(indice, ttl)
    tuberia.execute()
    return job_id, creado


def borrar(redis, job_id, usuario, archivos):
    """Deshace `crear` si no se pudo encolar."""
    claves = [f"job:{job_id}"] + [f"job:{job_id}:entrada:{a}" for a in range(archivos)]
    tuberia = redis.pipeline()
    tuberia.delete(*claves)
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
