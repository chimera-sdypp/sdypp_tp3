import json
import urllib.error
import urllib.request

import pytest

from comun import salud


@pytest.fixture
def levantar():
    """Levanta el /health en un puerto libre y devuelve una función que lo pide: (código, JSON)."""
    servidores = []

    def levantar_con(servicio, estado, ruta="/health"):
        servidor = salud.servir(servicio, puerto=0, estado=estado)
        servidores.append(servidor)
        url = f"http://127.0.0.1:{servidor.server_address[1]}{ruta}"

        def pedir():
            try:
                with urllib.request.urlopen(url, timeout=5) as respuesta:
                    return respuesta.status, json.loads(respuesta.read())
            except urllib.error.HTTPError as error:
                es_json = error.headers.get("Content-Type") == "application/json"
                return error.code, json.loads(error.read()) if es_json else None

        return pedir

    yield levantar_con
    for servidor in servidores:
        servidor.shutdown()
        servidor.server_close()


def test_con_las_dos_conexiones_ok_responde_200(levantar):
    pedir = levantar("split", {"rabbitmq": "ok", "redis": "ok"})

    assert pedir() == (200, {"split": "ok", "rabbitmq": "ok", "redis": "ok"})


@pytest.mark.parametrize("estado", [{"rabbitmq": "caido", "redis": "ok"}, {"rabbitmq": "ok", "redis": "caido"}])
def test_con_una_conexion_caida_responde_503(levantar, estado):
    pedir = levantar("split", estado)

    assert pedir() == (503, {"split": "ok", **estado})


def test_antes_del_primer_intento_de_conexion_las_dos_figuran_caidas(levantar):
    pedir = levantar("split", {})

    assert pedir() == (503, {"split": "ok", "rabbitmq": "caido", "redis": "caido"})


def test_otra_ruta_responde_404(levantar):
    pedir = levantar("split", {"rabbitmq": "ok", "redis": "ok"}, ruta="/metrics")

    assert pedir()[0] == 404
