import jwt
import pytest

from backend import auth
from backend.config import Config
from conftest import SECRETO, token


def test_sin_token_es_401(cliente):
    respuesta = cliente.get("/jobs")
    assert respuesta.status_code == 401
    assert respuesta.headers["www-authenticate"] == "Bearer"
    assert respuesta.json()["contenido"]["error"]["tipo"] == "NO_AUTORIZADO"


@pytest.mark.parametrize("cabecera", [
    "Bearer no-es-un-jwt",
    "Bearer " + jwt.encode({"sub": "ana", "role": "usuario", "exp": 9999999999}, "otro" * 8, algorithm="HS256"),
    "Bearer " + jwt.encode({"sub": "ana", "role": "usuario"}, SECRETO, algorithm="HS256"),
    "Basic YW5hOmNsYXZl",
])
def test_token_invalido_es_401(cliente, cabecera):
    assert cliente.get("/jobs", headers={"Authorization": cabecera}).status_code == 401


def test_token_vencido_es_401(cliente):
    respuesta = cliente.get("/jobs", headers=token(horas=-1))
    assert respuesta.status_code == 401
    assert respuesta.json()["contenido"]["error"]["mensaje"] == "el token venció"


def test_rol_desconocido_es_403(cliente):
    cabecera = jwt.encode({"sub": "ana", "role": "root", "exp": 9999999999}, SECRETO, algorithm="HS256")
    assert cliente.get("/jobs", headers={"Authorization": f"Bearer {cabecera}"}).status_code == 403


def test_usuario_en_stats_y_admin_es_403(cliente):
    for metodo, ruta in (("get", "/stats"), ("post", "/admin/dlq/reprocesar")):
        respuesta = getattr(cliente, metodo)(ruta, headers=token())
        assert respuesta.status_code == 403
        assert respuesta.json()["contenido"]["error"]["tipo"] == "ROL_INSUFICIENTE"


def test_limite_por_minuto_es_429(config, redis, rabbit, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from backend.app import crear_app
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    limitada = Config(secreto=SECRETO, imagenes_permitidas=config.imagenes_permitidas, pedidos_por_minuto=2)
    cliente = TestClient(crear_app(limitada, redis, rabbit, mantener=False))
    codigos = [cliente.get("/jobs", headers=token()).status_code for _ in range(3)]
    assert codigos == [200, 200, 429]
    respuesta = cliente.get("/jobs", headers=token())
    assert 1 <= int(respuesta.headers["retry-after"]) <= 60
    assert cliente.get("/jobs", headers=token("beto")).status_code == 200  # el límite es por usuario


def test_limite_cuenta_en_redis_y_vence_al_minuto(redis):
    reloj = lambda: 120.0
    auth.limitar(redis, "ana", 5, reloj=reloj)
    assert redis.get("limite:ana:2") == b"1"
    assert 0 < redis.ttl("limite:ana:2") <= 60


ENTORNO = {"SOBEL_SECRETO": SECRETO, "SOBEL_IMAGENES_PERMITIDAS": "a/b:1",
           "RABBITMQ_URL": "amqp://u:c@rabbitmq:5672/", "REDIS_URL": "redis://:c@redis:6379/0"}


@pytest.mark.parametrize("cambios, problema", [
    ({"SOBEL_SECRETO": "corto"}, "SOBEL_SECRETO"),
    ({"SOBEL_IMAGENES_PERMITIDAS": ""}, "vacía"),
    ({"SOBEL_IMAGENES_PERMITIDAS": "ghcr.io/a/b"}, "tag"),
    ({"SOBEL_IMAGENES_PERMITIDAS": "ghcr.io/a/b:latest"}, "tag"),
    ({"RABBITMQ_URL": ""}, "RABBITMQ_URL"),
    ({"REDIS_URL": ""}, "REDIS_URL"),
    ({"SOBEL_SERVICIOS": "backend=http://backend:8080/health"}, "backend"),
    ({"SOBEL_SERVICIOS": "split=http://split:8080"}, "/health"),
])
def test_config_no_arranca_mal_configurada(cambios, problema):
    with pytest.raises(ValueError, match=problema):
        Config.desde_entorno(ENTORNO | cambios)


def test_config_lee_listas():
    config = Config.desde_entorno(ENTORNO | {"SOBEL_IMAGENES_PERMITIDAS": "r:5000/a/b:1.0, c/d@sha256:abc",
                                             "SOBEL_SERVICIOS": "split=http://split:8080/health"})
    assert config.imagenes_permitidas == {"r:5000/a/b:1.0", "c/d@sha256:abc"}
    assert config.servicios == {"split": "http://split:8080/health"}
