"""GET /health de cada servicio (contrato §12), en un hilo aparte del consumo."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def servir(servicio, puerto, estado, conexiones=("rabbitmq", "redis")):
    """Levanta el servidor en `puerto` y devuelve el ThreadingHTTPServer (ya atendiendo).

    `estado` es el dict que actualiza `conexion.con_backoff`. Una conexión que todavía no figura ahí
    (el servicio recién arranca) cuenta como `caido`: 200 sólo si todas están `ok`, si no 503.
    """

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/health":
                self.send_error(404)
                return
            actual = {nombre: estado.get(nombre, "caido") for nombre in conexiones}
            cuerpo = json.dumps({servicio: "ok", **actual}).encode()
            self.send_response(200 if all(valor == "ok" for valor in actual.values()) else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(cuerpo)))
            self.end_headers()
            self.wfile.write(cuerpo)

        def log_message(self, formato, *args):
            pass  # las probes de k8s piden /health cada pocos segundos: no ensuciar los logs del servicio

    servidor = ThreadingHTTPServer(("0.0.0.0", puerto), Handler)
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    return servidor
