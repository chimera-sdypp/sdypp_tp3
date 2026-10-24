"""Frontend del Hit #3: la página para mandar jobs y verlos avanzar.

La página le habla a la API del backend en el mismo host (el ingress manda /jobs, /stats, /admin y /health al
backend). El avance llega por el fanout sobel.resultados: cada réplica le ata una cola exclusiva y reparte cada
resultado, por Server-Sent Events, a los navegadores que están mirando ese job.

uvicorn --factory frontend.app:crear_app
"""

import asyncio
import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import redis as redis_py
from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from comun import colas, conexion, metricas, registro

ESTATICO = Path(__file__).parent / "estatico"
LATIDO = 15  # segundos entre comentarios SSE: sin tráfico, el ingress corta la conexión
EN_ESPERA = 1000  # resultados que se guardan para un navegador lento; los que sobran se pierden
CSP = "default-src 'self'; img-src 'self' blob:; frame-ancestors 'none'"

ERRORES = metricas.ERRORES  # el común: registrarlo dos veces en el proceso no arranca
MIRANDO = Gauge("sobel_frontend_navegadores", "Navegadores con el avance de un job abierto")


class Difusor:
    """Reparte lo que llega de sobel.resultados entre los navegadores, por job_id."""

    def __init__(self):
        self.candado = threading.Lock()
        self.suscriptos = {}  # job_id → {(cola asyncio, su event loop)}

    def suscribir(self, job_id):
        suscripto = (asyncio.Queue(EN_ESPERA), asyncio.get_running_loop())
        with self.candado:
            self.suscriptos.setdefault(job_id, set()).add(suscripto)
        MIRANDO.inc()
        return suscripto

    def desuscribir(self, job_id, suscripto):
        with self.candado:
            restantes = self.suscriptos.get(job_id, set())
            restantes.discard(suscripto)
            if not restantes:
                self.suscriptos.pop(job_id, None)
        MIRANDO.dec()

    def publicar(self, resultado):
        """Lo llama el hilo de pika: pasa el resultado al event loop de cada navegador suscripto."""
        with self.candado:
            destinos = list(self.suscriptos.get(resultado.get("job_id"), ()))
        for cola, bucle in destinos:
            bucle.call_soon_threadsafe(_encolar, cola, resultado)


def _encolar(cola, resultado):
    try:
        cola.put_nowait(resultado)
    except asyncio.QueueFull:
        pass  # el navegador igual ve el avance en el GET /jobs/{id} que hace cada 2 s


def crear_app(entorno=None, redis=None, difusor=None, escuchar=True):
    entorno = os.environ if entorno is None else entorno
    backend = _obligatoria(entorno, "BACKEND_URL").rstrip("/")
    log, _ = registro.configurar("frontend")
    redis = redis or redis_py.Redis.from_url(_obligatoria(entorno, "REDIS_URL"), socket_timeout=2,
                                             socket_connect_timeout=2)
    difusor = difusor or Difusor()
    if escuchar:
        url_rabbit = _obligatoria(entorno, "RABBITMQ_URL")
        threading.Thread(target=_escuchar, args=(url_rabbit, difusor, log), daemon=True).start()
    app = FastAPI(title="Sobel — frontend", docs_url=None, redoc_url=None, openapi_url=None)
    app.mount("/estatico", StaticFiles(directory=ESTATICO), name="estatico")

    def consultar_job(job_id, autorizacion):
        """GET /jobs/{id} al backend con el token del navegador: sólo mira el avance quien puede ver el job."""
        pedido = urllib.request.Request(f"{backend}/jobs/{urllib.parse.quote(job_id, safe='')}")
        if autorizacion:
            pedido.add_header("Authorization", autorizacion)
        try:
            with urllib.request.urlopen(pedido, timeout=5) as respuesta:
                return respuesta.status, respuesta.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()
        except OSError as error:
            log.warning("el backend no responde (%r)", error)
            ERRORES.labels("frontend", "BACKEND_CAIDO").inc()
            return 503, json.dumps(_no_disponible("el backend no responde")).encode()

    @app.get("/")
    def pagina():
        return FileResponse(ESTATICO / "index.html")

    @app.get("/eventos/{job_id}")
    async def eventos(job_id: str, request: Request):
        codigo, cuerpo = await run_in_threadpool(consultar_job, job_id, request.headers.get("authorization"))
        if codigo != 200:
            return Response(cuerpo, codigo, media_type="application/json")
        suscripto = difusor.suscribir(job_id)
        cola, _ = suscripto

        async def flujo():
            try:
                while True:
                    try:
                        resultado = await asyncio.wait_for(cola.get(), LATIDO)
                    except TimeoutError:
                        yield ": latido\n\n"
                        continue
                    yield f"data: {json.dumps(resultado)}\n\n"
            finally:
                difusor.desuscribir(job_id, suscripto)

        return StreamingResponse(flujo(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/ofertas")
    def ofertas():
        """{imagen: [operaciones]} de los workers activos (contrato §10), para armar los pasos en la página."""
        resultado = {}
        try:
            for clave in redis.scan_iter(match="worker:*", count=100):
                datos = redis.hgetall(clave)
                imagen = datos.get(b"imagen", b"").decode()
                if imagen:
                    resultado.setdefault(imagen, set()).update(
                        o for o in datos.get(b"operaciones", b"").decode().split(",") if o)
        except (redis_py.exceptions.ConnectionError, redis_py.exceptions.TimeoutError) as error:
            log.warning("redis no responde (%r)", error)
            ERRORES.labels("frontend", "REDIS_CAIDO").inc()
            return JSONResponse(_no_disponible("no se pueden leer los workers activos"), 503,
                                headers={"Retry-After": "5"})
        return {"codigo": 200, "contenido": {"ofertas": {i: sorted(o) for i, o in sorted(resultado.items())}}}

    @app.get("/health")
    def health():
        try:
            estado_redis = conexion.OK if redis.ping() else conexion.CAIDO
        except Exception:
            estado_redis = conexion.CAIDO
        estados = {"frontend": conexion.OK, "rabbitmq": conexion.estado.get("rabbitmq", conexion.CAIDO),
                   "redis": estado_redis}
        return JSONResponse(estados, 200 if all(v == conexion.OK for v in estados.values()) else 503)

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.middleware("http")
    async def cabeceras(request: Request, siguiente):
        respuesta = await siguiente(request)
        respuesta.headers["Content-Security-Policy"] = CSP
        respuesta.headers["X-Content-Type-Options"] = "nosniff"
        return respuesta

    return app


def _escuchar(url, difusor, log):
    """Consume la cola exclusiva para siempre; `consumir` reconecta con backoff si se cae RabbitMQ."""
    def recibir(canal, cuerpo):
        try:
            resultado = json.loads(cuerpo)
        except ValueError:
            log.error("resultado ilegible en %s, lo descarto: %r", colas.RESULTADOS, cuerpo[:200])
            ERRORES.labels("frontend", "MENSAJE_ILEGIBLE").inc()
            return
        if isinstance(resultado, dict):
            difusor.publicar(resultado)

    def al_conectar(canal):
        colas.declarar(canal)
        return colas.declarar_exclusiva(canal)

    conexion.consumir(None, recibir, al_conectar=al_conectar, prefetch=100, servicio="frontend",
                      conectar=lambda: conexion.conectar_rabbitmq(url, log=log), log=log)


def _no_disponible(mensaje):
    """El sobre del error 503 del contrato (§6)."""
    return {"codigo": 503, "contenido": {"error": {"tipo": "SERVICIO_NO_DISPONIBLE", "mensaje": mensaje,
                                                   "detalles": []}}}


def _obligatoria(entorno, nombre):
    if not entorno.get(nombre):
        raise ValueError(f"falta la variable de entorno {nombre}")
    return entorno[nombre]
