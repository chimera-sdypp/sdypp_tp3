import json

import pytest

from backend import validacion
from conftest import IMAGEN, OTRA, enviar, pasos, png, ruido, token


def test_job_valido_se_guarda_y_se_encola(cliente, redis, rabbit, worker):
    respuesta = enviar(cliente, [png(), png(4, 4)], pasos("grises", "sobel"))
    assert respuesta.status_code == 202
    contenido = respuesta.json()["contenido"]
    job_id = contenido["job_id"]
    assert contenido["estado"] == "queued" and contenido["archivos"] == 2
    assert respuesta.headers["location"] == f"/jobs/{job_id}" == contenido["url"]
    assert rabbit.colas["sobel.split"] == [{"job_id": job_id}]
    job = redis.hgetall(f"job:{job_id}")
    assert job[b"usuario"] == b"ana" and job[b"estado"] == b"queued"
    assert json.loads(job[b"operaciones"])[1] == {"operacion": "sobel", "imagen": IMAGEN}
    assert redis.get(f"job:{job_id}:entrada:1") == png(4, 4)
    assert 0 < redis.ttl(f"job:{job_id}") <= 86400


def test_jpeg_tambien_vale(cliente, worker):
    import io
    from PIL import Image
    salida = io.BytesIO()
    Image.new("RGB", (5, 5)).save(salida, "JPEG")
    assert enviar(cliente, [salida.getvalue()]).status_code == 202


def tipo(respuesta):
    return respuesta.json()["contenido"]["error"]["tipo"]


def test_sin_multipart_es_415(cliente):
    respuesta = cliente.post("/jobs", json={"operaciones": []}, headers=token())
    assert respuesta.status_code == 415 and tipo(respuesta) == "TIPO_DE_CONTENIDO"


def test_multipart_cortado_es_400(cliente):
    cabeceras = {**token(), "Content-Type": "multipart/form-data; boundary=xyz"}
    respuesta = cliente.post("/jobs", content=b"esto no es multipart", headers=cabeceras)
    assert respuesta.status_code == 400 and tipo(respuesta) == "CUERPO_INVALIDO"


@pytest.mark.parametrize("operaciones, problema", [
    ("no es json", "no es JSON válido"),
    ("[]", "de 1 a 3 pasos"),
    (pasos("sobel", "sobel", "sobel", "sobel"), "llegó 4"),
    ('{"operacion": "sobel"}', "lista"),
    ('[{"operacion": "sobel"}]', "el paso 0"),
    ('[{"operacion": "sobel", "imagen": "x", "extra": 1}]', "el paso 0"),
    ('[{"operacion": 3, "imagen": "x"}]', "el paso 0"),
])
def test_operaciones_invalidas_son_422(cliente, worker, operaciones, problema):
    respuesta = enviar(cliente, operaciones=operaciones)
    assert respuesta.status_code == 422 and tipo(respuesta) == "PAYLOAD_INVALIDO"
    detalle, = respuesta.json()["contenido"]["error"]["detalles"]
    assert detalle["campo"] == "operaciones" and problema in detalle["problema"]


def test_faltan_campos_o_sobran(cliente):
    respuesta = cliente.post("/jobs", files=[("otro", ("x", b"1"))], headers=token())
    assert respuesta.status_code == 422
    campos = {d["campo"] for d in respuesta.json()["contenido"]["error"]["detalles"]}
    assert campos == {"otro", "archivos", "operaciones"}


def test_mas_de_50_archivos_es_422(cliente, worker):
    respuesta = enviar(cliente, [png()] * 51)
    assert respuesta.status_code == 422
    assert "llegaron 51" in respuesta.json()["contenido"]["error"]["detalles"][0]["problema"]


def test_archivo_que_no_es_imagen_o_esta_corrupto_es_422(cliente, worker):
    respuesta = enviar(cliente, [png(), b"GIF89a....", png()[:40]])
    assert respuesta.status_code == 422 and tipo(respuesta) == "ARCHIVO_INVALIDO"
    assert respuesta.json()["contenido"]["error"]["detalles"] == [
        {"campo": "archivos[1]", "problema": "no es PNG ni JPEG"},
        {"campo": "archivos[2]", "problema": "está corrupto"},
    ]


def test_archivo_grande_es_413(cliente, worker, monkeypatch):
    monkeypatch.setattr(validacion, "ARCHIVO_MAX", 100)
    respuesta = enviar(cliente, [png(), ruido(60, 60)])
    assert respuesta.status_code == 413 and "archivo 1 (a1.png)" in respuesta.json()["contenido"]["error"]["mensaje"]


def test_demasiados_pixeles_es_413(cliente, worker, monkeypatch):
    monkeypatch.setattr(validacion, "PIXELES_MAX", 99)
    respuesta = enviar(cliente, [png(10, 10)])
    assert respuesta.status_code == 413 and "10x10" in respuesta.json()["contenido"]["error"]["mensaje"]


def test_pedido_grande_es_413(cliente, worker, monkeypatch):
    monkeypatch.setattr(validacion, "PEDIDO_MAX", 1000)
    respuesta = enviar(cliente, [ruido(20, 20)] * 2)
    assert respuesta.status_code == 413


def test_imagen_no_permitida_es_403(cliente, worker):
    respuesta = enviar(cliente, operaciones=pasos("sobel", imagen="docker.io/alguien/minero:1"))
    assert respuesta.status_code == 403 and tipo(respuesta) == "IMAGEN_NO_PERMITIDA"


def test_imagen_sin_workers_es_503(cliente, worker):
    respuesta = enviar(cliente, operaciones=pasos("sobel", imagen=OTRA))
    assert respuesta.status_code == 503 and tipo(respuesta) == "SIN_WORKERS"
    assert respuesta.headers["retry-after"] == "10"


def test_operacion_que_la_imagen_no_ofrece_es_422(cliente, worker):
    respuesta = enviar(cliente, operaciones=pasos("rotar"))
    assert respuesta.status_code == 422 and tipo(respuesta) == "OPERACION_NO_SOPORTADA"
    assert "ofrece: blur, grises, sobel" in respuesta.json()["contenido"]["error"]["mensaje"]


def test_las_operaciones_se_detectan_de_los_workers_registrados(cliente, redis, worker):
    assert enviar(cliente, operaciones=pasos("umbral")).status_code == 422
    redis.hset("worker:w2", mapping={"imagen": IMAGEN, "operaciones": "umbral"})
    assert enviar(cliente, operaciones=pasos("umbral")).status_code == 202


def test_un_invalido_no_llega_a_rabbitmq(cliente, rabbit, worker):
    enviar(cliente, operaciones=pasos("rotar"))
    enviar(cliente, [b"nada"])
    assert rabbit.colas == {}


def test_rabbitmq_caido_es_503_y_no_deja_el_job(cliente, redis, rabbit, worker):
    rabbit.caido = True
    respuesta = enviar(cliente)
    assert respuesta.status_code == 503 and tipo(respuesta) == "SERVICIO_NO_DISPONIBLE"
    assert redis.keys("job:*") == [] and redis.zcard("jobs") == 0
