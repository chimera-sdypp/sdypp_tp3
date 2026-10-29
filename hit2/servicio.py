"""Hit #2 — el master del Hit #1 como servicio permanente en k3s, con desborde a la nube según la demanda.

    POST /pedidos?costo=N           cuerpo: la imagen → 202 {"id", "estado": "queued", "consulta"}
    GET  /pedidos/<id>              estado (queued, processing, done, failed), tiempos y workers que participaron
    GET  /pedidos/<id>/resultado    la imagen con bordes (PNG); 404 si todavía no está o ya venció
    GET  /estado                    en cola, atendidos por segundo, latencia media, disco usado y VMs
    GET  /health                    {"master": "ok", "rabbitmq": ..., "redis": ..., "disco": ...}
    GET  /metrics                   las mismas cuentas para Prometheus (prefijo sobel_hit2_)

La imagen se guarda en el disco del master (almacen.py, el volumen SOBEL_DATOS), el estado del pedido en Redis y a la
cola sobel.peticiones va sólo el id. Un consumidor toma los pedidos de a uno, reparte las franjas entre los workers
locales y las VMs que ya responden (el reparto del Hit #1, master.procesar), guarda el resultado en disco por
SOBEL_VENCIMIENTO segundos y avisa en sobel.resultados, que guarda como mucho SOBEL_MAX_RESULTADOS mensajes: RabbitMQ
descarta los más viejos. El escalador (escalador.py) mira sobel.peticiones y sube o baja las VMs.
"""

import io
import json
import os
import re
import secrets
import signal
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pika
import redis
from PIL import Image, UnidentifiedImageError
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

import almacen
import escalador
import master
import registro
import sobel

log, _ = registro.configurar("servicio")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://localhost:5672/")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
LOCALES = os.environ.get("SOBEL_LOCALES", "sobel-worker:8080")
TIMEOUT = float(os.environ.get("SOBEL_TIMEOUT", 300))
VENCIMIENTO = int(os.environ.get("SOBEL_VENCIMIENTO", 600))        # segundos que se guardan los resultados
MAX_RESULTADOS = int(os.environ.get("SOBEL_MAX_RESULTADOS", 100))
DATOS = os.environ.get("SOBEL_DATOS", "/datos")                     # el volumen con las imágenes
MAX_DISCO = int(float(os.environ.get("SOBEL_MAX_DISCO_GB", 20)) * 2**30)
MAXIMO_BYTES = 150 * 1024 * 1024
COLA_PETICIONES, COLA_RESULTADOS = "sobel.peticiones", "sobel.resultados"
ESPERAS = (1, 2, 4, 8, 16, 30)   # reconexión a RabbitMQ con backoff, con tope de 30 s

# Para Prometheus (hit2/k8s/monitoreo.yaml): el mismo Prometheus lee al Hit #3, de ahí el prefijo sobel_hit2_.
RECIBIDOS = Counter("sobel_hit2_pedidos_recibidos_total", "Pedidos aceptados por POST /pedidos")
ATENDIDOS = Counter("sobel_hit2_pedidos_atendidos_total", "Pedidos terminados", ["estado"])
TIEMPO = Histogram("sobel_hit2_tiempo_pedido_segundos", "Lo que tardó el reparto de cada pedido, según cuántos "
                   "workers lo atendieron", ["workers"], buckets=(2.5, 5, 7.5, 10, 12.5, 15, 17.5, 20, 25, 30, 45, 60))
LATENCIA = Histogram("sobel_hit2_latencia_segundos", "De la llegada al resultado: la espera en la cola más el reparto",
                     buckets=(5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 300, 600))
EN_COLA = Gauge("sobel_hit2_pedidos_en_cola", "Pedidos esperando en sobel.peticiones")
VMS = Gauge("sobel_hit2_vms", "VMs en GCP: las pedidas a Terraform y las listas (responden /health)", ["estado"])
UMBRAL = Gauge("sobel_hit2_umbral_pedidos", "Pedidos en espera por cada VM que lanza el escalador")
MAXIMO_VMS = Gauge("sobel_hit2_vms_maximo", "Tope de VMs del escalador")
DISCO = Gauge("sobel_hit2_disco_bytes", "Lo que ocupan las imágenes en el volumen del master")


def conectar(latido=60):
    parametros = pika.URLParameters(RABBITMQ_URL)
    parametros.heartbeat = latido
    conexion = pika.BlockingConnection(parametros)
    canal = conexion.channel()
    canal.queue_declare(COLA_PETICIONES, durable=True)
    canal.queue_declare(COLA_RESULTADOS, durable=True, arguments={"x-max-length": MAX_RESULTADOS})
    return conexion, canal


def publicar(cola, mensaje):
    """Una conexión por mensaje: los pedidos llegan de a pocos por segundo y así no hay que cuidar latidos."""
    conexion, canal = conectar()
    try:
        canal.basic_publish("", cola, json.dumps(mensaje),
                            pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent,
                                                 content_type="application/json"))
    finally:
        conexion.close()


def en_cola():
    conexion, canal = conectar()
    try:
        return canal.queue_declare(COLA_PETICIONES, passive=True).method.message_count
    finally:
        conexion.close()


class Master:
    """El estado compartido entre la API, el consumidor y el escalador."""

    def __init__(self, base):
        self.base = base                      # Redis: pedido:<id>, un hash con el estado
        self.archivos = almacen.Almacen(DATOS, VENCIMIENTO, MAX_RESULTADOS, MAX_DISCO)   # las imágenes
        self.hechos = deque(maxlen=1000)      # (fin, latencia) de los últimos pedidos, para /estado
        self.usando = []                      # los workers del pedido en curso (el escalador no borra esas VMs)
        self.escalador = escalador.desde_entorno(en_cola, lambda: list(self.usando))
        self.detener = threading.Event()
        EN_COLA.set_function(self._en_cola_o_nan)        # se calculan cuando Prometheus pide /metrics
        VMS.labels("pedidas").set_function(lambda: self.escalador.estado()["pedidas"])
        VMS.labels("listas").set_function(lambda: len(self.escalador.nube()))
        UMBRAL.set(self.escalador.umbral)
        MAXIMO_VMS.set(self.escalador.maximo)
        DISCO.set_function(self.archivos.usado)

    @staticmethod
    def _en_cola_o_nan():
        try:
            return en_cola()
        except pika.exceptions.AMQPError:   # sin RabbitMQ /metrics sigue respondiendo, con el hueco en el gráfico
            return float("nan")

    # ------------------------------------------------------------------ API
    def recibir(self, datos, costo, nombre):
        Image.open(io.BytesIO(datos))   # valida antes de encolar: lanza UnidentifiedImageError si no es imagen
        if not self.archivos.hay_lugar(len(datos)):
            raise almacen.SinLugar(f"la imagen pesa {len(datos) / 2**20:.0f} MiB y hay {self.archivos.usado() / 2**30:.1f}"
                                   f" de {MAX_DISCO / 2**30:.0f} GiB ocupados")
        id_ = secrets.token_hex(6)
        clave = f"pedido:{id_}"
        self.archivos.guardar(id_, "entrada", datos)
        with self.base.pipeline() as tuberia:
            tuberia.hset(clave, mapping={"estado": "queued", "llegada": time.time(), "costo": costo, "nombre": nombre})
            tuberia.expire(clave, VENCIMIENTO * 6)
            tuberia.execute()
        publicar(COLA_PETICIONES, {"id": id_})
        RECIBIDOS.inc()
        return id_

    def consultar(self, id_):
        datos = self.base.hgetall(f"pedido:{id_}")
        if not datos:
            return None
        estado = {k.decode(): v.decode() for k, v in datos.items()}
        for campo in ("llegada", "inicio", "fin"):
            if campo in estado:
                estado[campo] = float(estado[campo])
        if "costo" in estado:
            estado["costo"] = int(estado["costo"])
        if "workers" in estado:
            estado["workers"] = estado["workers"].split(",")
        if "fin" in estado:
            estado["latencia_s"] = round(estado["fin"] - estado["llegada"], 3)
        return estado

    def resultado(self, id_):
        return self.archivos.leer(id_, "resultado")

    def estadisticas(self):
        ahora = time.time()
        recientes = [latencia for fin, latencia in self.hechos if fin > ahora - 60]
        return {"en_cola": en_cola(), "atendidos_por_s": round(len(recientes) / 60, 3),
                "latencia_media_s": round(sum(recientes) / len(recientes), 1) if recientes else None,
                "atendidos": len(self.hechos), "disco_mb": round(self.archivos.usado() / 2**20),
                "locales": master.resolver_workers(LOCALES),
                "vms": self.escalador.estado()}

    # ------------------------------------------------------------------ consumidor
    def consumir(self):
        intento = 0
        while not self.detener.is_set():
            try:
                conexion, canal = conectar(latido=0)   # sin latidos: un pedido puede tardar minutos
                canal.basic_qos(prefetch_count=1)
                intento = 0
                log.info("consumiendo %s", COLA_PETICIONES)
                for metodo, _, cuerpo in canal.consume(COLA_PETICIONES, inactivity_timeout=1):
                    if self.detener.is_set():
                        break
                    if metodo is None:
                        continue
                    self.atender(json.loads(cuerpo)["id"])
                    canal.basic_ack(metodo.delivery_tag)   # recién ahora: si el master se cae, el pedido vuelve
                canal.cancel()
                conexion.close()
            except (pika.exceptions.AMQPError, redis.RedisError) as error:
                espera = ESPERAS[min(intento, len(ESPERAS) - 1)]
                log.warning("RabbitMQ o Redis no disponible (%s): reintento en %d s", error, espera)
                intento += 1
                self.detener.wait(espera)

    def atender(self, id_):
        clave = f"pedido:{id_}"
        costo = int(self.base.hget(clave, "costo") or 1)
        llegada = float(self.base.hget(clave, "llegada") or time.time())
        workers = master.resolver_workers(LOCALES) + self.escalador.nube()
        self.usando = workers
        inicio = time.time()
        self.base.hset(clave, mapping={"estado": "processing", "inicio": inicio, "workers": ",".join(workers)})
        try:
            datos = self.archivos.leer(id_, "entrada")
            if datos is None:
                raise ValueError("la imagen ya no está en el disco (venció antes de procesarse)")
            resultado = master.procesar(sobel.leer_gris(datos), workers, len(workers), TIMEOUT,
                                        redescubrir=lambda: master.resolver_workers(LOCALES), costo=costo)
            self.archivos.guardar(id_, "resultado", sobel.a_png(resultado))
        except (master.ErrorWorker, ValueError, OSError) as error:   # OSError: el disco (p. ej. lleno)
            estado = {"estado": "failed", "error": str(error)}
            log.error("pedido %s: %s", id_, error)
        else:
            estado = {"estado": "done"}
        finally:
            self.usando = []
        fin = time.time()
        estado["fin"] = fin
        self.base.hset(clave, mapping=estado)
        self.base.expire(clave, VENCIMIENTO)
        self.archivos.borrar(id_, "entrada")
        self.hechos.append((fin, fin - llegada))
        ATENDIDOS.labels(estado["estado"]).inc()
        if estado["estado"] == "done":
            TIEMPO.labels(str(len(workers))).observe(fin - inicio)
            LATENCIA.observe(fin - llegada)
        publicar(COLA_RESULTADOS, {"id": id_, "estado": estado["estado"], "latencia_s": round(fin - llegada, 3),
                                   "workers": len(workers)})
        log.info("pedido %s %s: esperó %.1f s y tardó %.1f s con %d workers (costo %d)", id_, estado["estado"],
                 inicio - llegada, fin - inicio, len(workers), costo)

    def limpiar(self):
        """Cada 30 s borra del disco lo vencido: a diferencia de Redis, los archivos no vencen solos."""
        while not self.detener.wait(30):
            try:
                borrados = self.archivos.limpiar()
            except OSError as error:
                log.error("no se pudo limpiar %s: %s", DATOS, error)
                continue
            if borrados:
                log.info("limpieza: %d imágenes vencidas o de más borradas del disco", borrados)

    def salud(self):
        partes = {"master": "ok"}
        try:
            self.base.ping()
            partes["redis"] = "ok"
        except redis.RedisError:
            partes["redis"] = "caído"
        try:
            en_cola()
            partes["rabbitmq"] = "ok"
        except pika.exceptions.AMQPError:
            partes["rabbitmq"] = "caído"
        partes["disco"] = "ok" if os.access(DATOS, os.W_OK) else "caído"
        return partes


class Handler(BaseHTTPRequestHandler):
    master = None   # lo asigna main()

    def _responder(self, codigo, cuerpo, tipo="application/json"):
        if tipo == "application/json":
            cuerpo = json.dumps(cuerpo).encode()
        self.send_response(codigo)
        self.send_header("Content-Type", tipo)
        self.send_header("Content-Length", str(len(cuerpo)))
        self.end_headers()
        self.wfile.write(cuerpo)

    def _error(self, codigo, mensaje):
        log.warning("%s %s → %d: %s", self.command, self.path, codigo, mensaje)
        self._responder(codigo, {"error": mensaje})

    def do_GET(self):
        ruta = urlsplit(self.path).path
        if ruta == "/health":
            partes = self.master.salud()
            self._responder(200 if all(v == "ok" for v in partes.values()) else 503, partes)
        elif ruta == "/estado":
            self._responder(200, self.master.estadisticas())
        elif ruta == "/metrics":
            self._responder(200, generate_latest(), CONTENT_TYPE_LATEST)
        elif encontrado := re.fullmatch(r"/pedidos/([0-9a-f]{12})(/resultado)?", ruta):
            id_, resultado = encontrado.groups()
            if resultado:
                png = self.master.resultado(id_)
                if png is None:
                    self._error(404, "el resultado no está listo o ya venció")
                else:
                    self._responder(200, png, "image/png")
                return
            estado = self.master.consultar(id_)
            if estado is None:
                self._error(404, "no existe ese pedido (o ya venció)")
            else:
                self._responder(200, {"id": id_, **estado})
        else:
            self._error(404, "ruta inexistente")

    def do_POST(self):
        url = urlsplit(self.path)
        if url.path != "/pedidos":
            self._error(404, "ruta inexistente")
            return
        parametros = parse_qs(url.query)
        try:
            costo = int(parametros.get("costo", ["1"])[0])
            largo = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._error(400, "costo o Content-Length inválido")
            return
        if not 1 <= costo <= 100:
            self._error(400, "el costo tiene que estar entre 1 y 100")
            return
        if largo > MAXIMO_BYTES:
            self._error(413, f"la imagen supera {MAXIMO_BYTES // 2**20} MiB")
            return
        try:
            id_ = self.master.recibir(self.rfile.read(largo), costo, parametros.get("nombre", [""])[0])
        except UnidentifiedImageError:
            self._error(400, "el cuerpo no es una imagen")
            return
        except almacen.SinLugar as error:
            self._error(507, f"no hay lugar en el disco para otra imagen ({error}): probá en un rato")
            return
        except (redis.RedisError, pika.exceptions.AMQPError, OSError) as error:
            self._error(503, f"cola o almacenamiento no disponible: {error}")
            return
        self._responder(202, {"id": id_, "estado": "queued", "consulta": f"/pedidos/{id_}"})

    def log_message(self, formato, *args):
        pass


class Servidor(ThreadingHTTPServer):
    daemon_threads = True


def main():
    estado = Master(redis.Redis.from_url(REDIS_URL))
    Handler.master = estado
    servidor = Servidor(("0.0.0.0", int(os.environ.get("SOBEL_PUERTO", 8080))), Handler)
    hilos = [threading.Thread(target=estado.consumir, name="consumidor"),
             threading.Thread(target=estado.escalador.correr, name="escalador")]
    threading.Thread(target=estado.limpiar, name="limpieza", daemon=True).start()
    for hilo in hilos:
        hilo.start()

    def apagar(senal, _frame):
        log.info("%s: deja de recibir pedidos, termina el que tiene en curso y borra las VMs",
                 signal.Signals(senal).name)
        estado.detener.set()
        threading.Thread(target=servidor.shutdown).start()

    signal.signal(signal.SIGTERM, apagar)
    signal.signal(signal.SIGINT, apagar)
    log.info("master escuchando en el puerto %d (locales: %s, hasta %d VMs)", servidor.server_address[1], LOCALES,
             estado.escalador.maximo)
    servidor.serve_forever()
    hilos[0].join()               # el consumidor termina el pedido en curso
    estado.escalador.apagar()     # teardown: 0 VMs
    hilos[1].join(timeout=5)
    log.info("master detenido")


if __name__ == "__main__":
    main()
