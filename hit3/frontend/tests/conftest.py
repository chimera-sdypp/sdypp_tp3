import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import fakeredis
import pytest
from fastapi.testclient import TestClient

from frontend.app import Difusor, crear_app

TOKEN = {"Authorization": "Bearer bueno"}


@pytest.fixture
def backend():
    """Un backend de mentira: GET /jobs/j1 con el token bueno da 200; sin token 401; el resto 404."""
    pedidos = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            pedidos.append((self.path, self.headers.get("Authorization")))
            if self.headers.get("Authorization") is None:
                codigo = 401
            elif self.path == "/jobs/j1" and self.headers["Authorization"] == TOKEN["Authorization"]:
                codigo = 200
            else:
                codigo = 404
            cuerpo = json.dumps({"codigo": codigo, "contenido": {"job_id": "j1"}}).encode()
            self.send_response(codigo)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(cuerpo)))
            self.end_headers()
            self.wfile.write(cuerpo)

        def log_message(self, *args):
            pass

    servidor = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    servidor.url, servidor.pedidos = f"http://127.0.0.1:{servidor.server_port}", pedidos
    yield servidor
    servidor.shutdown()


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


@pytest.fixture
def difusor():
    return Difusor()


@pytest.fixture
def cliente(backend, redis, difusor, tmp_path, monkeypatch):
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    app = crear_app({"BACKEND_URL": backend.url + "/", "REDIS_URL": "redis://x"}, redis, difusor, escuchar=False)
    with TestClient(app) as cliente:
        yield cliente
