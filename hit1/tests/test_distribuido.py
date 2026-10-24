"""Master + workers reales (HTTP en hilos), sin Docker."""

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import numpy as np
import pytest
from PIL import Image

import centralizado
import master
import sobel
import worker


@pytest.fixture
def workers():
    """Levanta 3 workers en puertos libres y cuenta cuántas franjas atiende cada uno."""
    servidores, atendidas = [], []
    for _ in range(3):
        contador = []

        class Contador(worker.Handler):
            def do_POST(self, contador=contador):
                contador.append(1)
                super().do_POST()

        servidor = ThreadingHTTPServer(("127.0.0.1", 0), Contador)
        threading.Thread(target=servidor.serve_forever, args=(0.05,), daemon=True).start()
        servidores.append(servidor)
        atendidas.append(contador)
    yield [f"127.0.0.1:{s.server_address[1]}" for s in servidores], atendidas
    for servidor in servidores:
        servidor.shutdown()
        servidor.server_close()


@pytest.fixture
def imagen(tmp_path):
    datos = np.random.default_rng(1).integers(0, 256, (60, 40, 3), dtype=np.uint8)
    ruta = tmp_path / "entrada.jpg"
    Image.fromarray(datos).save(ruta)
    return ruta


def pedir(url, datos=None):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=datos), timeout=5) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def test_health(workers):
    codigo, cuerpo = pedir(f"http://{workers[0][0]}/health")
    assert codigo == 200 and json.loads(cuerpo) == {"worker": "ok"}


def test_worker_aplica_sobel(workers):
    gris = np.random.default_rng(2).integers(0, 256, (10, 8), dtype=np.uint8)
    codigo, cuerpo = pedir(f"http://{workers[0][0]}/sobel", sobel.a_png(gris))
    assert codigo == 200
    assert np.array_equal(sobel.leer_gris(cuerpo), sobel.aplicar(gris))


def test_worker_rechaza_lo_que_no_es_imagen(workers):
    codigo, cuerpo = pedir(f"http://{workers[0][0]}/sobel", b"no soy una imagen")
    assert codigo == 400 and "error" in json.loads(cuerpo)


def test_ruta_inexistente(workers):
    assert pedir(f"http://{workers[0][0]}/otra")[0] == 404
    assert pedir(f"http://{workers[0][0]}/otra", b"x")[0] == 404


@pytest.mark.parametrize("partes", [1, 3, 5])
def test_distribuido_igual_a_centralizado(workers, partes):
    direcciones, atendidas = workers
    gris = np.random.default_rng(3).integers(0, 256, (50, 30), dtype=np.uint8)
    resultado = master.procesar(gris, direcciones, partes, timeout=5)
    assert np.array_equal(resultado, sobel.aplicar(gris))
    # Round-robin: franja i → worker i % 3.
    assert [len(a) for a in atendidas] == [len(range(i, partes, 3)) for i in range(3)]


def test_worker_caido(workers):
    with pytest.raises(master.ErrorWorker):
        master.procesar(np.zeros((10, 10), np.uint8), ["127.0.0.1:1"], 1, timeout=2)


def test_resolver_workers():
    assert master.resolver_workers("localhost:8080, 127.0.0.1:8080") == ["127.0.0.1:8080"]
    for invalida in ("sinpuerto", ":8080", "host:abc", "no-existe.invalid:8080"):
        with pytest.raises(ValueError):
            master.resolver_workers(invalida)


def test_master_y_centralizado_por_linea_de_comandos(workers, imagen, tmp_path):
    direcciones, _ = workers
    distribuida, central = tmp_path / "distribuida.png", tmp_path / "central.png"
    assert master.main([str(imagen), str(distribuida), "--workers", ",".join(direcciones)]) == 0
    assert centralizado.main([str(imagen), str(central)]) == 0
    assert np.array_equal(np.asarray(Image.open(distribuida)), np.asarray(Image.open(central)))


def test_errores_por_linea_de_comandos(workers, imagen, tmp_path):
    direcciones, _ = workers
    salida = str(tmp_path / "salida.png")
    roto = tmp_path / "roto.png"
    roto.write_bytes(b"no soy una imagen")
    assert centralizado.main([str(roto), salida]) == 2
    assert master.main([str(roto), salida, "--workers", direcciones[0]]) == 2
    assert master.main([str(imagen), salida, "--workers", direcciones[0], "--partes", "0"]) == 2
    assert master.main([str(imagen), salida, "--workers", "127.0.0.1:1"]) == 1
