"""Frontend del Hit #3: la interfaz web interactiva para enviar jobs y monitorear su avance en tiempo real.

========================================================================================
¿QUÉ HACE ESTE SERVICIO EN EL SISTEMA DISTRIBUIDO?
========================================================================================
1. Sirve la página web estática (HTML, JavaScript, CSS) al navegador del usuario.
2. Consulta las "ofertas" (filtros disponibles) leyendo qué workers están vivos en Redis.
3. Monitorea el avance de los trabajos mediante Server-Sent Events (SSE) en `/eventos/{job_id}`.

========================================================================================
MEJORA CLAVE PARA LA DEFENSA (FRONTEND 100% STATELESS):
========================================================================================
- ANTES: Cada réplica del frontend abría una cola exclusiva en RabbitMQ atada al fanout
  `sobel.resultados`. Si escalabas a 50 réplicas, RabbitMQ multiplicaba el tráfico 50 veces,
  y el estado del avance vivía atrapado en la memoria RAM del pod (StatefulSet encubierto).
- AHORA: El frontend es 100% Stateless. No se conecta obligatoriamente a RabbitMQ. El endpoint
  SSE consulta periódicamente a Redis (`job:<id>`), donde el Joiner ya actualiza atómicamente
  los campos `completos` y `fragmentos_total`. Cualquier réplica puede atender a cualquier
  usuario en cualquier momento, tolerando caídas y escalando libremente.

Para ejecutar localmente con uvicorn:
    uvicorn --factory frontend.app:crear_app
"""

# Módulo para operaciones asincrónicas y bucles de eventos (event loops)
import asyncio
# Serialización y deserialización de mensajes en formato JSON
import json
# Lectura de variables de entorno del sistema operativo (REDIS_URL, BACKEND_URL, etc.)
import os
# Manejo de concurrencia mediante hilos (threads)
import threading
# Manejo de errores de red y HTTP nativos de Python
import urllib.error
# Codificación segura de URLs (escapado de caracteres especiales)
import urllib.parse
# Cliente HTTP nativo para consultar al backend sin librerías pesadas externas
import urllib.request
# Manejo orientado a objetos de rutas en el sistema de archivos
from pathlib import Path

# Cliente de conexión hacia la base de datos en memoria Redis
import redis as redis_py
# Framework web asincrónico para construir APIs rápidas y eficientes
from fastapi import FastAPI, Request
# Ejecuta funciones sincrónicas bloqueantes en un pool de hilos para no frenar FastAPI
from fastapi.concurrency import run_in_threadpool
# Respuestas HTTP especializadas: archivos, JSON, texto plano y flujos SSE
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
# Servidor de archivos estáticos (para entregar index.html, app.js y estilo.css)
from fastapi.staticfiles import StaticFiles
# Biblioteca oficial para exportar métricas en formato estándar de Prometheus
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

# Módulos comunes del proyecto: claves de Redis, colas RabbitMQ, conexión, métricas y logs
from comun import claves, colas, conexion, metricas, registro

# Ruta al directorio 'estatico/' donde residen los recursos web de la interfaz
ESTATICO = Path(__file__).parent / "estatico"

# Cada cuántos segundos enviar un comentario SSE (': latido') para que el Ingress no corte la conexión
LATIDO = 15

# Cada cuánto tiempo (en segundos) consultar a Redis para detectar nuevos fragmentos listos
INTERVALO_REDIS = 0.5

# Cantidad máxima de mensajes encolados en memoria para un navegador lento antes de descartar
EN_ESPERA = 1000

# Cabecera de seguridad Content-Security-Policy (CSP) para prevenir ataques de inyección (XSS)
CSP = "default-src 'self'; img-src 'self' blob:; frame-ancestors 'none'"

# Contador global de errores categorizados exportados a Prometheus
ERRORES = metricas.ERRORES

# Métrica Gauge de Prometheus: cantidad actual de navegadores conectados viendo un job
MIRANDO = Gauge("sobel_frontend_navegadores", "Navegadores con el avance de un job abierto")


class Difusor:
    """Administrador en memoria para repartir eventos a navegadores suscriptos por job_id.

    Se mantiene para compatibilidad con tests unitarios y para permitir entrega inmediata
    si se utiliza un hilo de escucha directo.
    """

    def __init__(self):
        # Candado (Lock) para sincronizar el acceso al diccionario entre múltiples hilos
        self.candado = threading.Lock()
        # Diccionario que mapea cada job_id con el conjunto de colas asyncio de los navegadores
        self.suscriptos = {}  # job_id → {(cola asyncio, su event loop)}

    def suscribir(self, job_id):
        """Registra un nuevo navegador interesado en las actualizaciones de un job_id."""
        # Se crea una cola de asyncio con límite y se obtiene el event loop actual
        suscripto = (asyncio.Queue(EN_ESPERA), asyncio.get_running_loop())
        with self.candado:
            # Agregamos la tupla (cola, loop) al conjunto de ese job_id
            self.suscriptos.setdefault(job_id, set()).add(suscripto)
        # Incrementamos el contador de usuarios mirando en Prometheus
        MIRANDO.inc()
        return suscripto

    def desuscribir(self, job_id, suscripto):
        """Remueve al navegador cuando cierra la pestaña o corta la conexión HTTP."""
        with self.candado:
            restantes = self.suscriptos.get(job_id, set())
            restantes.discard(suscripto)
            # Si ya no queda nadie mirando este job_id, liberamos la entrada del diccionario
            if not restantes:
                self.suscriptos.pop(job_id, None)
        # Decrementamos la métrica en Prometheus
        MIRANDO.dec()

    def publicar(self, resultado):
        """Entrega un resultado a todos los event loops de los navegadores que miran ese job."""
        with self.candado:
            destinos = list(self.suscriptos.get(resultado.get("job_id"), ()))
        for cola, bucle in destinos:
            # Planificamos la inserción en la cola de manera segura entre hilos (thread-safe)
            bucle.call_soon_threadsafe(_encolar, cola, resultado)


def _encolar(cola, resultado):
    """Encola un resultado en la cola de asyncio de un navegador.

    Si la cola se llena (porque el navegador o la conexión son muy lentos), se descarta el
    evento puntual sin bloquear el hilo ni colapsar la memoria; el navegador recuperará el
    estado general en su próxima consulta periódica a GET /jobs/{id}.
    """
    try:
        cola.put_nowait(resultado)
    except asyncio.QueueFull:
        pass


def crear_app(entorno=None, redis=None, difusor=None, escuchar=False):
    """Fábrica (factory) que configura e inicializa la aplicación web FastAPI del frontend.

    Parámetros:
    - entorno: variables de entorno (por defecto os.environ).
    - redis: cliente de Redis inyectable (útil para pruebas con fakeredis).
    - difusor: difusor en memoria para eventos en tiempo real.
    - escuchar: bandera booleana. Por defecto False (arquitectura Stateless recomendada por
      la cátedra). Si es True, levanta un hilo para consumir de RabbitMQ con cola exclusiva.
    """
    # 1. Obtenemos las variables de entorno del sistema
    entorno = os.environ if entorno is None else entorno

    # 2. Obtenemos la URL del backend (ej. http://backend:8080) quitando barras finales sobrantes
    backend = _obligatoria(entorno, "BACKEND_URL").rstrip("/")

    # 3. Configuramos el sistema de logs unificado del proyecto
    log, _ = registro.configurar("frontend")

    # 4. Establecemos la conexión con la base de datos Redis
    redis = redis or redis_py.Redis.from_url(
        _obligatoria(entorno, "REDIS_URL"),
        socket_timeout=2,
        socket_connect_timeout=2
    )

    # 5. Instanciamos el difusor en memoria
    difusor = difusor or Difusor()

    # 6. Modo opcional RabbitMQ: solo si se pide explícitamente (en producción es 100% Stateless con Redis)
    if escuchar:
        url_rabbit = _obligatoria(entorno, "RABBITMQ_URL")
        threading.Thread(target=_escuchar, args=(url_rabbit, difusor, log), daemon=True).start()

    # 7. Creamos la aplicación FastAPI deshabilitando docs automáticas para reducir superficie de ataque
    app = FastAPI(title="Sobel — frontend", docs_url=None, redoc_url=None, openapi_url=None)

    # 8. Montamos la carpeta de archivos estáticos en la ruta /estatico
    app.mount("/estatico", StaticFiles(directory=ESTATICO), name="estatico")

    def consultar_job(job_id, autorizacion):
        """Consulta al backend si el usuario tiene permiso para ver este job (GET /jobs/{id}).

        Reenvía la cabecera 'Authorization' con el token JWT del usuario.
        Retorna (codigo_http, cuerpo_bytes).
        """
        pedido = urllib.request.Request(f"{backend}/jobs/{urllib.parse.quote(job_id, safe='')}")
        if autorizacion:
            pedido.add_header("Authorization", autorizacion)
        try:
            with urllib.request.urlopen(pedido, timeout=5) as respuesta:
                return respuesta.status, respuesta.read()
        except urllib.error.HTTPError as error:
            # Errores 401 (no autorizado), 403 (prohibido) o 404 (no existe) del backend
            return error.code, error.read()
        except OSError as error:
            # Fallo de red si el backend está caído o inalcanzable
            log.warning("el backend no responde (%r)", error)
            ERRORES.labels("frontend", "BACKEND_CAIDO").inc()
            return 503, json.dumps(_no_disponible("el backend no responde")).encode()

    @app.get("/")
    def pagina():
        """Ruta principal: sirve el archivo HTML interactivo index.html al usuario."""
        return FileResponse(ESTATICO / "index.html")

    @app.get("/eventos/{job_id}")
    async def eventos(job_id: str, request: Request):
        """Endpoint Server-Sent Events (SSE) para transmitir el avance del job al navegador.

        Flujo 100% Stateless:
        1. Valida contra el backend que el usuario esté autenticado y tenga acceso al job.
        2. Inicia un flujo continuo consultando el avance en Redis (`job:<id>`).
        3. Cuando el Joiner procesa fragmentos y actualiza `completos`, este endpoint emite
           un evento SSE `data: {"completos": X, "total": Y, "porcentaje": Z}`.
        4. No guarda estado en memoria local del pod, por lo que cualquier réplica de frontend
           puede atender a cualquier cliente y reconectar sin perder progreso.
        """
        # 1. Verificamos la autorización con el backend en un hilo separado para no trabar el bucle async
        codigo, cuerpo = await run_in_threadpool(consultar_job, job_id, request.headers.get("authorization"))
        if codigo != 200:
            return Response(cuerpo, codigo, media_type="application/json")

        # 2. Suscribimos la sesión local (si hay difusor) y actualizamos la métrica en Prometheus
        suscripto = difusor.suscribir(job_id) if difusor else None
        cola = suscripto[0] if suscripto else None
        if not suscripto:
            MIRANDO.inc()

        async def flujo():
            """Generador asincrónico que produce las líneas SSE enviadas al navegador."""
            clave_job = claves.job(job_id)  # Clave estandarizada: 'job:<job_id>'
            ultimo_completos = -1
            ultimo_estado = None
            tiempo_sin_actividad = 0.0

            try:
                while True:
                    emitio = False

                    # Paso A: si hay eventos encolados en memoria (ej. por tests o difusor), los enviamos
                    if cola:
                        while not cola.empty():
                            try:
                                item = cola.get_nowait()
                                yield f"data: {json.dumps(item)}\n\n"
                                emitio = True
                            except asyncio.QueueEmpty:
                                break

                    # Paso B: leemos el progreso directamente desde el Hash de Redis
                    try:
                        datos = await run_in_threadpool(redis.hgetall, clave_job)
                    except Exception as error:
                        log.warning("error al leer avance de redis para %s: %r", job_id, error)
                        datos = {}

                    # Paso C: si el hash existe en Redis, analizamos si hubo novedades
                    if datos:
                        def obtener(k):
                            # Soporta claves como string o bytes según la configuración del cliente Redis
                            val = datos.get(k) if k in datos else datos.get(k.encode())
                            if isinstance(val, bytes):
                                return val.decode("utf-8", errors="replace")
                            return str(val) if val is not None else ""

                        estado = obtener("estado") or "processing"
                        try:
                            total = int(obtener("fragmentos_total") or 0)
                        except ValueError:
                            total = 0
                        try:
                            completos = int(obtener("completos") or 0)
                        except ValueError:
                            completos = 0

                        # Paso D: si cambió la cantidad de fragmentos listos o el estado, emitimos a SSE
                        if completos != ultimo_completos or estado != ultimo_estado:
                            ultimo_completos = completos
                            ultimo_estado = estado
                            porcentaje = int((completos / total) * 100) if total > 0 else 0
                            payload = {
                                "job_id": job_id,
                                "estado": estado,
                                "completos": completos,
                                "total": total,
                                "porcentaje": porcentaje,
                            }
                            yield f"data: {json.dumps(payload)}\n\n"
                            emitio = True

                            # Si el trabajo ya terminó (con éxito o error), cerramos el flujo SSE
                            if estado in ("done", "failed"):
                                break

                    # Paso E: control del latido periódico (Heartbeat) para evitar cierres de Ingress
                    if emitio:
                        tiempo_sin_actividad = 0.0
                    else:
                        tiempo_sin_actividad += INTERVALO_REDIS

                    if tiempo_sin_actividad >= LATIDO:
                        yield ": latido\n\n"
                        tiempo_sin_actividad = 0.0

                    # Paso F: esperamos el intervalo antes del siguiente sondeo o despertamos si hay evento
                    if cola:
                        try:
                            item = await asyncio.wait_for(cola.get(), timeout=INTERVALO_REDIS)
                            yield f"data: {json.dumps(item)}\n\n"
                            tiempo_sin_actividad = 0.0
                        except (TimeoutError, asyncio.TimeoutError):
                            pass
                    else:
                        await asyncio.sleep(INTERVALO_REDIS)
            finally:
                # Paso G: al desconectarse el navegador, liberamos recursos y decrementamos la métrica
                if suscripto:
                    difusor.desuscribir(job_id, suscripto)
                else:
                    MIRANDO.dec()

        # Retornamos una respuesta con cabeceras que deshabilitan el buffering intermedio de proxies
        return StreamingResponse(
            flujo(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    @app.get("/ofertas")
    def ofertas():
        """Devuelve los filtros disponibles agrupados por imagen de worker activo (Contrato §10).

        Permite que la interfaz web arme dinámicamente los selectores de pasos solo con
        las imágenes y operaciones soportadas por los workers que están vivos actualmente.
        """
        resultado = {}
        try:
            # Buscamos en Redis todas las claves que empiezan con 'worker:'
            for clave in redis.scan_iter(match="worker:*", count=100):
                datos = redis.hgetall(clave)
                imagen = datos.get(b"imagen", b"").decode()
                if imagen:
                    # Agregamos las operaciones que ofrece este worker separadas por coma
                    resultado.setdefault(imagen, set()).update(
                        o for o in datos.get(b"operaciones", b"").decode().split(",") if o
                    )
        except (redis_py.exceptions.ConnectionError, redis_py.exceptions.TimeoutError) as error:
            log.warning("redis no responde (%r)", error)
            ERRORES.labels("frontend", "REDIS_CAIDO").inc()
            return JSONResponse(_no_disponible("no se pueden leer los workers activos"), 503,
                                headers={"Retry-After": "5"})
        return {"codigo": 200, "contenido": {"ofertas": {i: sorted(o) for i, o in sorted(resultado.items())}}}

    @app.get("/health")
    def health():
        """Endpoint de salud (Health Check) consumido por Kubernetes (Liveness y Readiness Probes).

        Verifica:
        1. 'frontend': ok (si el proceso web puede responder solicitudes HTTP).
        2. 'redis': ok (si Redis responde al comando ping()).
        3. 'rabbitmq': solo si el frontend fue configurado con escuchar=True. De esta forma,
           si el frontend es Stateless y no usa RabbitMQ, no fallará falsamente si RabbitMQ titila.
        """
        try:
            estado_redis = conexion.OK if redis.ping() else conexion.CAIDO
        except Exception:
            estado_redis = conexion.CAIDO
        estados = {"frontend": conexion.OK, "redis": estado_redis}
        if escuchar:
            estados["rabbitmq"] = conexion.estado.get("rabbitmq", conexion.CAIDO)
        return JSONResponse(estados, 200 if all(v == conexion.OK for v in estados.values()) else 503)

    @app.get("/metrics")
    def metrics():
        """Expone las métricas del frontend en formato OpenMetrics/Prometheus para el scraper."""
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.middleware("http")
    async def cabeceras(request: Request, siguiente):
        """Middleware HTTP para inyectar cabeceras de seguridad indispensables en cada respuesta."""
        respuesta = await siguiente(request)
        respuesta.headers["Content-Security-Policy"] = CSP
        respuesta.headers["X-Content-Type-Options"] = "nosniff"
        return respuesta

    return app


def _escuchar(url, difusor, log):
    """Hilo secundario opcional: consume una cola exclusiva en RabbitMQ atada al fanout sobel.resultados."""
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
    """Genera la estructura estándar del sobre de error HTTP 503 según el Contrato de la cátedra (§6)."""
    return {
        "codigo": 503,
        "contenido": {
            "error": {
                "tipo": "SERVICIO_NO_DISPONIBLE",
                "mensaje": mensaje,
                "detalles": []
            }
        }
    }


def _obligatoria(entorno, nombre):
    """Valida la presencia de una variable de entorno requerida; si falta, aborta con un mensaje claro."""
    if not entorno.get(nombre):
        raise ValueError(f"falta la variable de entorno {nombre}")
    return entorno[nombre]
