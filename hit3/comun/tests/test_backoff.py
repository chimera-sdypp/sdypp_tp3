import logging

import pika.exceptions
import pytest
import redis.exceptions

from comun import conexion


def conectar_que_falla(veces, conexion_ok="conexión"):
    """Devuelve un `conectar` que tira error de conexión `veces` veces y después anda."""
    llamadas = []

    def conectar():
        llamadas.append(1)
        if len(llamadas) <= veces:
            raise pika.exceptions.AMQPConnectionError("broker caído")
        return conexion_ok

    return conectar


def test_espera_exponencial_con_tope_de_30_segundos():
    esperas = []

    resultado = conexion.con_backoff(conectar_que_falla(7), "rabbitmq", dormir=esperas.append)

    assert esperas == [1, 2, 4, 8, 16, 30, 30]
    assert resultado == "conexión"


def test_loguea_cada_reconexion_con_su_espera(caplog):
    caplog.set_level(logging.INFO)

    conexion.con_backoff(conectar_que_falla(3), "rabbitmq", dormir=lambda s: None,
                         log=logging.getLogger("test"))

    avisos = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert avisos == [
        "reconexión 1 a rabbitmq falló (AMQPConnectionError: ('broker caído',)), espero 1 s",
        "reconexión 2 a rabbitmq falló (AMQPConnectionError: ('broker caído',)), espero 2 s",
        "reconexión 3 a rabbitmq falló (AMQPConnectionError: ('broker caído',)), espero 4 s",
    ]
    assert "conectado a rabbitmq" in caplog.text


def test_con_intentos_max_se_rinde_y_propaga_el_error():
    esperas = []

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        conexion.con_backoff(conectar_que_falla(10), "rabbitmq", dormir=esperas.append, intentos_max=3)

    assert esperas == [1, 2, 4]


@pytest.mark.parametrize("error", [
    pika.exceptions.ProbableAuthenticationError("usuario o contraseña incorrectos"),
    pika.exceptions.ProbableAccessDeniedError("sin permiso en el vhost"),
    redis.exceptions.AuthenticationError("invalid password"),
])
def test_credenciales_invalidas_cortan_sin_reintentar(error):
    llamadas, esperas = [], []

    def conectar():
        llamadas.append(1)
        raise error

    with pytest.raises(type(error)):
        conexion.con_backoff(conectar, "rabbitmq", dormir=esperas.append, intentos_max=3)

    assert len(llamadas) == 1
    assert esperas == []


def test_estado_muestra_caido_mientras_espera_y_ok_al_conectar():
    visto_mientras_espera = []

    conexion.con_backoff(conectar_que_falla(2), "redis",
                         dormir=lambda s: visto_mientras_espera.append(conexion.estado["redis"]))

    assert visto_mientras_espera == ["caido", "caido"]
    assert conexion.estado["redis"] == "ok"
