"""Backend del Hit #3: la API pública del contrato (Parte 1).

uvicorn --factory backend.app:crear_app
"""

import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import redis as redis_py
from fastapi import Depends, FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from starlette.exceptions import HTTPException
from starlette.routing import Match

from backend import auth, jobs, validacion
from backend.config import Config
from backend.errores import ErrorApi, respuesta_de_error, sobre
from backend.rabbit import NoDisponible, Rabbit
from comun import colas, registro

PEDIDOS = Counter("sobel_pedidos_total", "Pedidos HTTP al backend", ["ruta", "codigo"])
ERRORES = Counter("sobel_errores_total", "Errores por tipo", ["servicio", "tipo"])
EN_COLA = Gauge("sobel_tareas_en_cola", "Mensajes en cada cola", ["cola"])
CADA = 10  # segundos entre actualizaciones de sobel_tareas_en_cola (y latidos a RabbitMQ)


def crear_app(config=None, redis=None, rabbit=None, mantener=True):
    config = config or Config.desde_entorno()
    log, _ = registro.configurar("backend")
    redis = redis or redis_py.Redis.from_url(config.redis_url, socket_timeout=5, socket_connect_timeout=2)
    rabbit = rabbit or Rabbit(config.rabbitmq_url, log=log)
    app = FastAPI(title="Sobel — backend", docs_url=None, redoc_url=None, openapi_url=None, redirect_slashes=False)

    def identidad(request: Request):
        ident = auth.autenticar(request.headers.get("authorization"), config.secreto)
        auth.limitar(redis, ident.usuario, config.pedidos_por_minuto)
        return ident

    def job_visible(job_id, ident):
        job = jobs.leer(redis, job_id)
        if job is None or (job["usuario"] != ident.usuario and not ident.es_admin):
            raise ErrorApi("JOB_INEXISTENTE", f"no existe el job {job_id}")
        return job

    @app.post("/jobs")
    async def crear_job(request: Request, ident=Depends(identidad)):
        if not request.headers.get("content-type", "").startswith("multipart/form-data"):
            raise ErrorApi("TIPO_DE_CONTENIDO", "el cuerpo tiene que ser multipart/form-data "
                           f"(llegó: {request.headers.get('content-type') or 'nada'})")
        if int(request.headers.get("content-length") or 0) > validacion.PEDIDO_MAX:
            raise ErrorApi("ARCHIVO_DEMASIADO_GRANDE", f"el pedido supera los {validacion.PEDIDO_MAX} bytes")
        formulario = await request.form(max_files=validacion.ARCHIVOS_MAX + 1)  # mal formado: 400 de Starlette
        archivos, operaciones = validacion.campos(formulario)
        validacion.tamanos(archivos)
        contenidos = [await archivo.read() for archivo in archivos]
        if sum(map(len, contenidos)) > validacion.PEDIDO_MAX:
            raise ErrorApi("ARCHIVO_DEMASIADO_GRANDE", f"el pedido supera los {validacion.PEDIDO_MAX} bytes")
        # Lo pesado (decodificar imágenes, Redis, RabbitMQ) va a un hilo para no frenar los demás pedidos.
        nombres = [archivo.filename for archivo in archivos]
        return await run_in_threadpool(aceptar, ident, operaciones, contenidos, nombres)

    def aceptar(ident, operaciones, contenidos, nombres):
        validacion.imagenes(contenidos, nombres)
        validacion.pasos(operaciones, config.imagenes_permitidas, jobs.ofertas(redis))
        job_id, creado = jobs.crear(redis, config.ttl, ident.usuario, operaciones, contenidos)
        try:
            rabbit.publicar(colas.SPLIT, {"job_id": job_id})
        except NoDisponible:
            jobs.borrar(redis, job_id, ident.usuario, len(contenidos))
            raise
        log.info("job %s de %s: %d archivos, %s", job_id, ident.usuario, len(contenidos),
                 " → ".join(p["operacion"] for p in operaciones))
        return sobre(202, {"job_id": job_id, "estado": "queued", "url": f"/jobs/{job_id}",
                           "archivos": len(contenidos), "creado": jobs.iso(creado)},
                     {"Location": f"/jobs/{job_id}"})

    @app.get("/jobs")
    def listar_jobs(estado: str | None = None, usuario: str | None = None, ident=Depends(identidad)):
        if estado is not None and estado not in jobs.ESTADOS:
            raise ErrorApi("PAYLOAD_INVALIDO", "el filtro no es válido",
                           [{"campo": "estado", "problema": f"tiene que ser uno de {', '.join(jobs.ESTADOS)}"}])
        de_quien = (usuario or None) if ident.es_admin else ident.usuario
        return sobre(200, {"jobs": jobs.listar(redis, config.ttl, de_quien, estado, limite=100)})

    @app.get("/jobs/{job_id}")
    def ver_job(job_id: str, ident=Depends(identidad)):
        return sobre(200, jobs.vista(redis, job_id, job_visible(job_id, ident)))

    @app.get("/jobs/{job_id}/resultado/{n}")
    def resultado(job_id: str, n: str, ident=Depends(identidad)):
        job = job_visible(job_id, ident)
        if not n.isdigit() or int(n) >= int(job["archivos"]):
            raise ErrorApi("RUTA_INEXISTENTE", f"no existe la ruta /jobs/{job_id}/resultado/{n}")
        if job["estado"] != "done":
            porcentaje = jobs.vista(redis, job_id, job)["fragmentos"]["porcentaje"]
            raise ErrorApi("JOB_NO_TERMINADO", f"el job está en {job['estado']} ({porcentaje} %)")
        datos = redis.get(f"job:{job_id}:resultado:{n}")
        if datos is None:
            raise ErrorApi("JOB_INEXISTENTE", f"el resultado {n} del job {job_id} ya no está")
        return Response(datos, media_type="image/png")

    @app.get("/stats")
    def stats(ident=Depends(identidad)):
        auth.exigir_admin(ident, "/stats")
        cuentas = rabbit.contar(config.imagenes_permitidas)
        datos = jobs.estadisticas(redis, config.ttl)
        return sobre(200, {
            "jobs": datos["jobs"],
            "colas": {cola: n for cola, n in cuentas.items() if cola not in (colas.DLQ, colas.AGOTADOS)},
            "dlq": {"fallidos": cuentas[colas.DLQ], "agotados": cuentas[colas.AGOTADOS]},
            "workers_activos": datos["workers_activos"],
            "latencia_s": datos["latencia_s"],
            "tasa_error": datos["tasa_error"],
        })

    @app.post("/admin/dlq/reprocesar")
    def reprocesar(ident=Depends(identidad)):
        auth.exigir_admin(ident, "/admin/dlq/reprocesar")
        movidos = rabbit.reprocesar_agotados()
        jobs.volver_a_procesar(redis, movidos)
        log.info("%s reprocesó %d fragmentos de la DLQ", ident.usuario, len(movidos))
        return sobre(200, {"reprocesados": len(movidos), "jobs": sorted(set(movidos))})

    @app.get("/health")
    def health():
        estados = {"backend": "ok", "redis": _estado(lambda: redis.ping()),
                   "rabbitmq": "ok" if rabbit.vivo() else "caido"}
        with ThreadPoolExecutor(max_workers=max(len(config.servicios), 1)) as hilos:
            for nombre, valor in zip(config.servicios, hilos.map(_consultar, config.servicios.values())):
                estados[nombre] = valor
        estados["workers"] = _estado(lambda: jobs.workers(redis) or None)
        codigo = 200 if estados["redis"] == estados["rabbitmq"] == "ok" else 503
        return sobre(codigo, estados)

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.middleware("http")
    async def contar_pedidos(request: Request, siguiente):
        inicio = time.perf_counter()
        respuesta = await siguiente(request)
        ruta = getattr(request.scope.get("route"), "path", "otra")
        PEDIDOS.labels(ruta, str(respuesta.status_code)).inc()
        if ruta not in ("/health", "/metrics"):
            log.info("%s %s → %d (%.0f ms)", request.method, request.url.path, respuesta.status_code,
                     (time.perf_counter() - inicio) * 1000)
        return respuesta

    @app.exception_handler(ErrorApi)
    async def error_api(request, error):
        ERRORES.labels("backend", error.tipo).inc()
        return respuesta_de_error(error)

    @app.exception_handler(HTTPException)
    async def error_http(request, error):
        if error.status_code == 405:
            # Starlette sólo informa los métodos de la primera ruta que coincide (/jobs tiene dos)
            permitidos = {metodo for ruta in app.routes if ruta.matches(request.scope)[0] != Match.NONE
                          for metodo in getattr(ruta, "methods", None) or ()} - {"HEAD"}
            return await error_api(request, ErrorApi(
                "METODO_NO_PERMITIDO", f"{request.method} no está permitido en {request.url.path}",
                cabeceras={"Allow": ", ".join(sorted(permitidos))}))
        if error.status_code == 400:  # el multipart no se pudo leer
            return await error_api(request, ErrorApi("CUERPO_INVALIDO",
                                                     f"el cuerpo multipart está mal formado ({error.detail})"))
        return await error_api(request, ErrorApi("RUTA_INEXISTENTE", f"no existe la ruta {request.url.path}"))

    @app.exception_handler(NoDisponible)
    @app.exception_handler(redis_py.exceptions.ConnectionError)
    @app.exception_handler(redis_py.exceptions.TimeoutError)
    async def no_disponible(request, error):
        log.warning("%s %s: %r", request.method, request.url.path, error)
        return await error_api(request, ErrorApi("SERVICIO_NO_DISPONIBLE",
                                                 "el servicio no puede aceptar jobs en este momento",
                                                 cabeceras={"Retry-After": "5"}))

    @app.exception_handler(Exception)
    async def inesperado(request, error):
        log.exception("error inesperado en %s %s", request.method, request.url.path)
        return await error_api(request, ErrorApi("ERROR_INTERNO", "error interno del servidor"))

    if mantener:
        threading.Thread(target=_mantener, args=(rabbit, config, log), daemon=True).start()
    return app


def _mantener(rabbit, config, log):
    """Cada CADA segundos: latido a RabbitMQ (lo reconecta si se cayó) y sobel_tareas_en_cola."""
    while True:
        try:
            for cola, n in rabbit.contar(config.imagenes_permitidas).items():
                EN_COLA.labels(cola).set(n)
        except NoDisponible:
            pass
        except Exception:
            log.exception("no se pudo actualizar sobel_tareas_en_cola")
        time.sleep(CADA)


def _estado(prueba):
    try:
        return "ok" if prueba() else "caido"
    except Exception:
        return "caido"


def _consultar(url):
    try:
        with urllib.request.urlopen(url, timeout=1) as respuesta:
            return "ok" if respuesta.status == 200 else "caido"
    except Exception:
        return "caido"
