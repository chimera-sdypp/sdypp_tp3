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
    def __init__(self, codigo, contenido=b"", cabeceras=None):
        self.status_code, self.content, self.text = codigo, contenido, contenido.decode(errors="replace")
        self.headers = cabeceras or {}


class SesionFalsa:
    """Lo que `Almacen` usa de requests: anota cada pedido y devuelve la respuesta que le toque."""

    def __init__(self, *respuestas):
        self.pedidos = []
        self.cabeceras = []
        self.respuestas = list(respuestas)

    def request(self, metodo, url, **opciones):
        self.pedidos.append((metodo, url, opciones.get("params")))
        self.cabeceras.append(opciones.get("headers"))
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


SESION_GCS = ("https://storage.googleapis.com/upload/storage/v1/b/imagenes/o?uploadType=resumable"
              "&name=job%2Fj1%2Fentrada%2F0.png&upload_id=ABC")


def test_iniciar_subida_abre_una_sesion_resumable_que_exige_el_tamano_declarado():
    sesion = SesionFalsa(Respuesta(200, cabeceras={"Location": SESION_GCS}))
    url = Almacen("imagenes", sesion=sesion).iniciar_subida("job/j1/entrada/0.png", 1234)

    assert url == SESION_GCS
    assert sesion.pedidos == [("POST", "https://storage.googleapis.com/upload/storage/v1/b/imagenes/o",
                               {"uploadType": "resumable", "name": "job/j1/entrada/0.png"})]
    assert sesion.cabeceras[0] == {"X-Upload-Content-Length": "1234"}


def test_con_el_emulador_la_url_de_subida_apunta_a_donde_lo_ve_el_cliente():
    interna = "http://0.0.0.0:4443/upload/storage/v1/b/imagenes/o?uploadType=resumable&upload_id=ABC"
    sesion = SesionFalsa(Respuesta(200, cabeceras={"Location": interna}))
    bucket = Almacen("imagenes", url="http://almacen:4443", sesion=sesion, url_publica="http://localhost:4443")
    assert (bucket.iniciar_subida("job/j1/entrada/0.png", 10)
            == "http://localhost:4443/upload/storage/v1/b/imagenes/o?uploadType=resumable&upload_id=ABC")


def test_sin_location_la_subida_no_se_pudo_abrir():
    with pytest.raises(NoDisponible):
        Almacen("imagenes", sesion=SesionFalsa(Respuesta(200))).iniciar_subida("a.png", 10)


def test_inicio_pide_solo_un_rango_y_saca_el_tamano_total_de_content_range():
    sesion = SesionFalsa(Respuesta(206, b"\x89PNG", {"Content-Range": "bytes 0-3/104857600"}), Respuesta(404),
                         Respuesta(200, b"chico"))
    bucket = Almacen("imagenes", sesion=sesion)

    assert bucket.inicio("job/j1/entrada/0.png", cuantos=4) == (104857600, b"\x89PNG")
    assert sesion.cabeceras[0] == {"Range": "bytes=0-3"}
    assert bucket.inicio("job/j1/entrada/1.png") is None
    assert bucket.inicio("job/j1/entrada/2.png") == (5, b"chico")  # más chico que el rango: vino entero


def test_inicio_en_memoria(memoria):
    memoria.guardar("a.png", b"0123456789")
    assert memoria.inicio("a.png", cuantos=4) == (10, b"0123")
    assert memoria.inicio("b.png") is None


def test_la_configuracion_dinamica_pisa_los_valores_por_defecto(redis, monkeypatch):
    from comun import dinamico
    monkeypatch.setenv("SOBEL_MAX_PARTES", "20")
    monkeypatch.setenv("SOBEL_PNG_NIVEL", "99")  # fuera de rango: queda el del código
    assert dinamico.leer(redis)["max_partes"] == 20 and dinamico.leer(redis)["png_nivel"] == 1

    redis.hset("config:dinamica", mapping={"max_partes": 4, "png_nivel": "x", "otra": 3})  # inválidas: se ignoran
    assert dinamico.personalizadas(redis) == {"max_partes": 4}
    assert dinamico.leer(redis)["max_partes"] == 4

    dinamico.actualizar(redis, {"max_partes": None, "hilos_almacen": 2})
    assert dinamico.personalizadas(redis) == {"hilos_almacen": 2} and dinamico.leer(redis)["max_partes"] == 20


def test_la_configuracion_dinamica_valida_todo_junto():
    from comun import dinamico
    assert dinamico.validar({"png_nivel": 9, "max_partes": None}) == ({"png_nivel": 9, "max_partes": None}, [])
    _, detalles = dinamico.validar({"png_nivel": 10, "max_partes": True, "otra": 1})
    assert [d["campo"] for d in detalles] == ["png_nivel", "max_partes", "otra"]
    assert dinamico.validar({})[1] and dinamico.validar([1])[1]


def test_sin_redis_la_configuracion_es_la_por_defecto():
    from comun import dinamico

    class RedisCaido:
        def hgetall(self, clave):
            raise ConnectionError("caído")

    assert dinamico.leer(RedisCaido()) == dinamico.por_defecto()
