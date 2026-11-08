import fakeredis
import pytest
import requests

from comun import almacen
from comun.almacen import Almacen, AlmacenMemoria, NoDisponible


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


@pytest.fixture
def memoria():
    return AlmacenMemoria()


def test_el_nombre_del_objeto_sale_de_la_clave_de_redis():
    assert almacen.nombre_de("job:j1:frag:0:2:1") == "job/j1/frag/0/2/1.png"


def test_en_redis_queda_el_puntero_y_en_el_bucket_los_bytes(redis, memoria):
    almacen.escribir(redis, memoria, "job:j1:resultado:0", b"png", ttl=60)

    assert redis.get("job:j1:resultado:0") == b"job/j1/resultado/0.png"
    assert memoria.objetos == {"job/j1/resultado/0.png": b"png"}
    assert 0 < redis.ttl("job:j1:resultado:0") <= 60
    assert almacen.leer(redis, memoria, "job:j1:resultado:0") == b"png"


def test_sin_puntero_no_hay_datos(redis, memoria):
    assert almacen.leer(redis, memoria, "job:j1:resultado:0") is None


def test_un_puntero_a_un_objeto_que_ya_no_esta_es_como_no_tener_datos(redis, memoria):
    # La regla de vencimiento del bucket lo borró antes que el TTL de Redis.
    redis.set("job:j1:resultado:0", "job/j1/resultado/0.png")
    assert almacen.leer(redis, memoria, "job:j1:resultado:0") is None


def test_escribir_varios_y_leer_varios_respetan_el_orden(redis, memoria):
    pares = [(f"job:j1:frag:0:{i}:0", bytes([i])) for i in range(20)]
    almacen.escribir_varios(redis, memoria, pares, ttl=60)

    claves = [clave for clave, _ in pares] + ["job:j1:frag:0:99:0"]
    assert almacen.leer_varios(redis, memoria, claves) == [bytes([i]) for i in range(20)] + [None]


def test_borrar_saca_el_puntero_y_el_objeto(redis, memoria):
    almacen.escribir(redis, memoria, "job:j1:entrada:0", b"a", ttl=60)
    almacen.escribir(redis, memoria, "job:j1:entrada:1", b"b", ttl=60)

    almacen.borrar(redis, memoria, "job:j1:entrada:0", "job:j1:entrada:1", "job:j1:entrada:2")

    assert not redis.exists("job:j1:entrada:0", "job:j1:entrada:1") and memoria.objetos == {}


def test_si_el_bucket_no_responde_no_queda_un_puntero_colgado(redis, memoria):
    memoria.caido = True
    with pytest.raises(NoDisponible):
        almacen.escribir(redis, memoria, "job:j1:entrada:0", b"a", ttl=60)
    assert not redis.exists("job:j1:entrada:0")


def test_no_disponible_se_reintenta_como_una_caida_de_conexion():
    from comun import conexion
    assert issubclass(NoDisponible, conexion.ERRORES_DE_CONEXION)


class Respuesta:
    def __init__(self, codigo, contenido=b""):
        self.status_code, self.content, self.text = codigo, contenido, contenido.decode(errors="replace")


class SesionFalsa:
    """Lo que `Almacen` usa de requests: anota cada pedido y devuelve la respuesta que le toque."""

    def __init__(self, *respuestas):
        self.pedidos = []
        self.respuestas = list(respuestas)

    def request(self, metodo, url, **opciones):
        self.pedidos.append((metodo, url, opciones.get("params")))
        respuesta = self.respuestas.pop(0)
        if isinstance(respuesta, Exception):
            raise respuesta
        return respuesta


def test_guardar_sube_el_objeto_por_la_api_json_de_gcs():
    sesion = SesionFalsa(Respuesta(200))
    Almacen("imagenes", sesion=sesion).guardar("job/j1/entrada/0.png", b"png")
    assert sesion.pedidos == [("POST", "https://storage.googleapis.com/upload/storage/v1/b/imagenes/o",
                               {"uploadType": "media", "name": "job/j1/entrada/0.png"})]


def test_leer_codifica_el_nombre_y_un_404_es_que_no_existe():
    sesion = SesionFalsa(Respuesta(200, b"png"), Respuesta(404))
    bucket = Almacen("imagenes", url="http://emulador:4443/", sesion=sesion)

    assert bucket.leer("job/j1/entrada/0.png") == b"png"
    assert bucket.leer("job/j1/entrada/1.png") is None
    assert sesion.pedidos[0][1] == "http://emulador:4443/storage/v1/b/imagenes/o/job%2Fj1%2Fentrada%2F0.png"


@pytest.mark.parametrize("respuesta", [Respuesta(503, b"ocupado"), Respuesta(403, b"sin permiso"),
                                       requests.ConnectionError("sin red")])
def test_un_error_del_bucket_es_no_disponible(respuesta):
    with pytest.raises(NoDisponible):
        Almacen("imagenes", sesion=SesionFalsa(respuesta)).leer("job/j1/entrada/0.png")


def test_borrar_algo_que_ya_no_esta_no_es_un_error():
    sesion = SesionFalsa(Respuesta(404), Respuesta(204))
    Almacen("imagenes", sesion=sesion).borrar(["a.png", "b.png"])
    assert [p[0] for p in sesion.pedidos] == ["DELETE", "DELETE"]


def test_vivo_recuerda_el_resultado_para_no_preguntar_en_cada_probe():
    sesion = SesionFalsa(Respuesta(200))
    bucket = Almacen("imagenes", sesion=sesion)
    assert bucket.vivo() and bucket.vivo()
    assert len(sesion.pedidos) == 1


def test_asegurar_bucket_sólo_crea_en_el_emulador():
    sesion = SesionFalsa(Respuesta(409))  # ya existía
    Almacen("imagenes", url="http://emulador:4443", sesion=sesion).asegurar_bucket()
    assert sesion.pedidos == [("POST", "http://emulador:4443/storage/v1/b", {"project": "local"})]

    sin_pedidos = SesionFalsa()
    Almacen("imagenes", sesion=sin_pedidos).asegurar_bucket()
    assert sin_pedidos.pedidos == []


def test_desde_entorno_exige_el_bucket(monkeypatch):
    monkeypatch.delenv("SOBEL_ALMACEN_BUCKET", raising=False)
    with pytest.raises(RuntimeError, match="SOBEL_ALMACEN_BUCKET"):
        almacen.desde_entorno()
    monkeypatch.setenv("SOBEL_ALMACEN_BUCKET", "imagenes")
    monkeypatch.setenv("SOBEL_ALMACEN_URL", "http://emulador:4443")
    bucket = almacen.desde_entorno()
    assert (bucket.bucket, bucket.url, bucket.emulador) == ("imagenes", "http://emulador:4443", True)
