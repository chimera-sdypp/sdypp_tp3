"""Etapa 3: workers que se caen o no responden, con master y workers reales en hilos."""

import socket
import threading
import time
from http.server import ThreadingHTTPServer

import numpy as np
import pytest

import master
import sobel
import worker


class Contador(worker.Handler):
    """Worker sano que cuenta las franjas que procesó."""
    atendidas = None

    def do_POST(self):
        self.atendidas.append(1)
        super().do_POST()


class Caido(worker.Handler):
    """SOBEL_FALLA=caida, pero cortando la conexión en vez de terminar el proceso de pytest."""
    falla = "caida"

    def _caer(self):
        self.connection.shutdown(socket.SHUT_RDWR)
        self.close_connection = True


class Colgado(worker.Handler):
    falla = "colgado"
    liberar = threading.Event()  # propio: el fixture lo libera al terminar


class Error500(worker.Handler):
    def do_POST(self):
        self._responder(500, {"error": "explotó"})


class NoImagen(worker.Handler):
    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        self._responder(200, b"no soy un png", "image/png")


class Rechaza(worker.Handler):
    def do_POST(self):
        self._responder(400, {"error": "franja inválida"})


@pytest.fixture
def levantar():
    """levantar(Handler) → 'host:puerto' de un worker en un hilo; se apagan al final."""
    servidores = []

    def _levantar(handler, atendidas=None):
        if atendidas is not None:
            handler = type("ContadorPropio", (handler,), {"atendidas": atendidas})
        servidor = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=servidor.serve_forever, args=(0.05,), daemon=True).start()
        servidores.append(servidor)
        return f"127.0.0.1:{servidor.server_address[1]}"

    yield _levantar
    Colgado.liberar.set()
    for servidor in servidores:
        servidor.shutdown()
        servidor.server_close()
    Colgado.liberar.clear()


GRIS = np.random.default_rng(7).integers(0, 256, (60, 40), dtype=np.uint8)


def test_reasigna_las_franjas_de_un_worker_caido(levantar):
    sanos = [[], []]
    direcciones = [levantar(Contador, sanos[0]), levantar(Caido), levantar(Contador, sanos[1])]

    resultado = master.procesar(GRIS, direcciones, 6, timeout=5)

    assert np.array_equal(resultado, sobel.aplicar(GRIS))
    assert sum(len(a) for a in sanos) == 6  # las 6 franjas las terminaron los sanos


def test_reasigna_si_un_worker_no_responde(levantar):
    direcciones = [levantar(Contador, []), levantar(Colgado)]

    inicio = time.perf_counter()
    resultado = master.procesar(GRIS, direcciones, 4, timeout=0.5)

    assert np.array_equal(resultado, sobel.aplicar(GRIS))
    # Las franjas del colgado vencen en paralelo: se espera un timeout, no uno por franja.
    assert time.perf_counter() - inicio < 2


@pytest.mark.parametrize("handler", [Error500, NoImagen])
def test_respuestas_rotas_cuentan_como_caida(levantar, handler):
    direcciones = [levantar(handler), levantar(Contador, [])]
    assert np.array_equal(master.procesar(GRIS, direcciones, 4, timeout=5), sobel.aplicar(GRIS))


def test_sin_workers_vivos_el_trabajo_falla(levantar):
    direcciones = [levantar(Caido), levantar(Colgado), "127.0.0.1:1"]
    with pytest.raises(master.ErrorWorker, match="no quedan workers"):
        master.procesar(GRIS, direcciones, 3, timeout=0.5)


def test_redescubre_workers_nuevos_por_dns(levantar):
    nuevo = levantar(Contador, [])
    resultado = master.procesar(GRIS, [levantar(Caido)], 3, timeout=5, redescubrir=lambda: [nuevo])
    assert np.array_equal(resultado, sobel.aplicar(GRIS))


def test_redescubrir_sin_resultados_no_oculta_el_error(levantar):
    def dns_caido():
        raise ValueError("no se pudo resolver el worker")

    with pytest.raises(master.ErrorWorker):
        master.procesar(GRIS, [levantar(Caido)], 2, timeout=5, redescubrir=dns_caido)


def test_un_4xx_no_se_reasigna(levantar):
    otro = []
    direcciones = [levantar(Rechaza), levantar(Contador, otro)]
    with pytest.raises(master.ErrorFranja):
        master.procesar(GRIS, direcciones, 1, timeout=5)
    assert otro == []


def test_round_robin_sin_fallas_no_cambia():
    disponibles = master.Workers(["a", "b", "c"])
    assert [disponibles.elegir(i) for i in range(6)] == ["a", "b", "c", "a", "b", "c"]


def test_elegir_saltea_los_descartados():
    disponibles = master.Workers(["a", "b", "c"])
    disponibles.descartar("b")
    assert {disponibles.elegir(1) for _ in range(4)} == {"a", "c"}
    assert disponibles.elegir(0) == "a"


def test_master_reasigna_por_linea_de_comandos(levantar, tmp_path):
    from PIL import Image
    entrada, salida = tmp_path / "entrada.png", tmp_path / "salida.png"
    Image.fromarray(GRIS).save(entrada)
    direcciones = ",".join([levantar(Caido), levantar(Contador, [])])

    assert master.main([str(entrada), str(salida), "--workers", direcciones, "--partes", "4"]) == 0
    assert np.array_equal(np.asarray(Image.open(salida)), sobel.aplicar(GRIS))


def test_worker_rechaza_falla_desconocida(monkeypatch):
    monkeypatch.setattr(worker.Handler, "falla", "explotar")
    with pytest.raises(SystemExit):
        worker.main(["--puerto", "0"])
