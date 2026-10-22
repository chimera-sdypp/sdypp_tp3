"""GET /health y GET /metrics de cada servicio (contrato §11 y §12), en un hilo aparte del consumo."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from comun import metricas
from comun.conexion import CAIDO, OK

CONEXIONES = ("rabbitmq", "redis")


def servir(servicio, puerto, estado, redis=None):
    """Levanta el servidor en `puerto` y devuelve el ThreadingHTTPServer (ya atendiendo).

    `estado` es el dict que actualiza `conexion.con_backoff`. Una conexión que todavía no figura ahí
    (el servicio recién arranca) cuenta como `caido`: 200 sólo si todas están `ok`, si no 503.
    Con `redis`, además le hace ping: `estado` cambia recién al reconectar, y un servicio ocioso no
    toca Redis, así que sin esto una caída no se vería hasta el próximo mensaje.
    """

    def conexiones():
        actual = {nombre: estado.get(nombre, CAIDO) for nombre in CONEXIONES}
        if redis is not None and actual["redis"] == OK:
            try:
                redis.ping()
            except Exception:
                actual["redis"] = CAIDO
        return actual

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            ruta = self.path.split("?", 1)[0]
            if ruta == "/metrics":  # responde aunque las conexiones estén caídas: es cuando más sirve
                tipo, cuerpo = metricas.exponer()
                self._enviar(200, tipo, cuerpo)
                return
            if ruta != "/health":
                self._responder(404, {"error": f"no existe la ruta {self.path}"})
                return
            actual = conexiones()
            self._responder(200 if all(valor == OK for valor in actual.values()) else 503,
                            {servicio: OK, **actual})

        def _responder(self, codigo, contenido):
            self._enviar(codigo, "application/json", json.dumps(contenido).encode())

        def _enviar(self, codigo, tipo, cuerpo):
            self.send_response(codigo)
            self.send_header("Content-Type", tipo)
            self.send_header("Content-Length", str(len(cuerpo)))
            self.end_headers()
            self.wfile.write(cuerpo)

        def log_message(self, formato, *args):
            pass  # las probes de k8s piden /health cada pocos segundos: no ensuciar los logs del servicio

    servidor = ThreadingHTTPServer(("0.0.0.0", puerto), Handler)
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    return servidor
