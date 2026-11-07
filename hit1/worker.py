"""Etapa 2 — worker: aplica Sobel a la franja que le manda el master.

    POST /sobel   cuerpo: la franja como imagen (PNG)  →  200 con el resultado en PNG
    GET  /health  →  {"worker": "ok"}

POST /sobel?costo=N repite el cálculo N veces (1 a 100): simula una tarea N veces más pesada
con el mismo resultado.

Para probar la Etapa 3 (tolerancia a fallos) el worker puede simular fallos:
    SOBEL_FALLA=caida     el proceso termina al recibir una franja, sin responder
    SOBEL_FALLA=colgado   recibe la franja y nunca responde (/health sigue contestando)
    SOBEL_DEMORA=<s>      demora artificial por franja, para tener tiempo de matar un worker a mano

Con SIGTERM (lo que manda k8s o `docker stop` antes de matar el contenedor) deja de aceptar
franjas, termina las que tiene en curso y sale. Sin el handler, Python como PID 1 ignora
SIGTERM y el contenedor recién muere con el SIGKILL del final del período de gracia.
"""

import argparse
import json
import os
import signal
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import registro
import sobel

MAXIMO_BYTES = 64 * 1024 * 1024
COSTO_MAXIMO = 100
NOMBRE = socket.gethostname()
FALLAS = ("", "caida", "colgado")
log, _ = registro.configurar("worker")


class Handler(BaseHTTPRequestHandler):
    falla = os.environ.get("SOBEL_FALLA", "")
    demora = float(os.environ.get("SOBEL_DEMORA", 0))
    liberar = threading.Event()  # un worker "colgado" espera esto, que en producción nunca llega

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
        if self.path == "/health":
            self._responder(200, {"worker": "ok"})
        else:
            self._error(404, "ruta inexistente")

    def do_POST(self):
        url = urlsplit(self.path)
        if url.path != "/sobel":
            self._error(404, "ruta inexistente")
            return
        try:
            costo = int(parse_qs(url.query).get("costo", ["1"])[0])
        except ValueError:
            costo = 0
        if not 1 <= costo <= COSTO_MAXIMO:
            self._error(400, f"el costo tiene que ser un entero entre 1 y {COSTO_MAXIMO}")
            return
        try:
            largo = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._error(411, "falta Content-Length")
            return
        if largo > MAXIMO_BYTES:
            self._error(413, f"la franja supera {MAXIMO_BYTES} bytes")
            return
        inicio = time.perf_counter()
        try:
            gris = sobel.leer_gris(self.rfile.read(largo))
        except (OSError, ValueError):
            self._error(400, "el cuerpo no es una imagen")
            return
        if self.falla == "caida":
            log.error("SOBEL_FALLA=caida: el worker %s termina sin responder la franja", NOMBRE)
            self._caer()
            return
        if self.falla == "colgado":
            log.error("SOBEL_FALLA=colgado: el worker %s no va a responder la franja", NOMBRE)
            self.liberar.wait()
            return
        if self.demora:
            time.sleep(self.demora)
        for _ in range(costo):
            filtrada = sobel.aplicar(gris)
        resultado = sobel.a_png(filtrada)
        log.info("franja %dx%d procesada en %.3f s (costo %d)", gris.shape[1], gris.shape[0],
                 time.perf_counter() - inicio, costo)
        self._responder(200, resultado, "image/png")

    def _caer(self):
        os._exit(1)  # como un crash: sin cerrar sockets ni responder

    def log_message(self, formato, *args):
        pass  # cada pedido ya queda en el log propio


class Servidor(ThreadingHTTPServer):
    daemon_threads = False  # server_close() espera a que terminen los pedidos en curso


def main(argv=None):
    parser = argparse.ArgumentParser(description="Worker de Sobel (HTTP).")
    parser.add_argument("--puerto", type=int, default=int(os.environ.get("SOBEL_PUERTO", 8080)))
    args = parser.parse_args(argv)
    if Handler.falla not in FALLAS:
        parser.error(f"SOBEL_FALLA inválida: {Handler.falla!r} (opciones: caida, colgado)")
    servidor = Servidor(("0.0.0.0", args.puerto), Handler)

    def apagar(senal, _frame):
        log.info("%s: el worker deja de aceptar franjas y termina las que tiene en curso",
                 signal.Signals(senal).name)
        Handler.liberar.set()  # suelta las franjas colgadas: server_close() espera a todos los hilos
        # shutdown() espera a que serve_forever() salga: llamarlo desde este mismo hilo lo traba.
        threading.Thread(target=servidor.shutdown).start()

    signal.signal(signal.SIGTERM, apagar)
    signal.signal(signal.SIGINT, apagar)
    log.info("worker %s escuchando en el puerto %d%s", NOMBRE, args.puerto,
             f" (simulando falla: {Handler.falla})" if Handler.falla else "")
    servidor.serve_forever()
    servidor.server_close()
    log.info("worker %s detenido", NOMBRE)


if __name__ == "__main__":
    main()
