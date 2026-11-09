"""Subida directa al bucket: POST /jobs/presigned + PUT a la URL + POST /jobs/{id}/confirmar (contrato §2b)."""

import json
import struct
import zlib

import pytest

from backend import validacion
from comun import almacen
from conftest import IMAGEN, png, token


def operaciones(*nombres):
    return [{"operacion": o, "imagen": IMAGEN} for o in nombres or ("sobel",)]


def reservar(cliente, tamanos, ops=None, cabeceras=None):
    return cliente.post("/jobs/presigned", json={"archivos": tamanos, "operaciones": ops or operaciones()},
                        headers=cabeceras or token())


def subir(bucket, job_id, *archivos):
    """Lo que hace el cliente con cada URL: el objeto aparece en el bucket con el nombre que eligió el backend."""
    for a, datos in enumerate(archivos):
        bucket.guardar(f"job/{job_id}/entrada/{a}.png", datos)


def confirmar(cliente, job_id, cabeceras=None):
    return cliente.post(f"/jobs/{job_id}/confirmar", headers=cabeceras or token())


def tipo(respuesta):
    return respuesta.json()["contenido"]["error"]["tipo"]


def test_reservar_abre_una_subida_por_archivo_y_todavia_no_crea_el_job(cliente, redis, rabbit, bucket, worker):
    tamanos = [len(png()), len(png(4, 4))]
    respuesta = reservar(cliente, tamanos, operaciones("grises", "sobel"))

    assert respuesta.status_code == 200
    contenido = respuesta.json()["contenido"]
    job_id = contenido["job_id"]
    assert [(s["archivo"], s["bytes"], s["metodo"]) for s in contenido["subidas"]] == [(0, tamanos[0], "PUT"),
                                                                                       (1, tamanos[1], "PUT")]
    assert contenido["confirmar"] == f"/jobs/{job_id}/confirmar"
    # Cada sesión exige exactamente los bytes declarados, y sólo para el objeto que eligió el backend.
    assert bucket.subidas == {f"job/{job_id}/entrada/0.png": tamanos[0], f"job/{job_id}/entrada/1.png": tamanos[1]}
    assert not redis.exists(f"job:{job_id}") and redis.zcard("jobs") == 0 and rabbit.colas == {}
    assert 0 < redis.ttl(f"job:{job_id}:reserva") <= 900


def test_subir_y_confirmar_encola_el_job_sin_que_los_bytes_pasen_por_el_backend(cliente, redis, rabbit, bucket,
                                                                                 worker):
    job_id = reservar(cliente, [len(png()), len(png(4, 4))]).json()["contenido"]["job_id"]
    subir(bucket, job_id, png(), png(4, 4))

    respuesta = confirmar(cliente, job_id)

    assert respuesta.status_code == 202
    contenido = respuesta.json()["contenido"]
    assert contenido["job_id"] == job_id and contenido["estado"] == "queued" and contenido["archivos"] == 2
    assert respuesta.headers["location"] == f"/jobs/{job_id}"
    assert rabbit.colas["sobel.split"] == [{"job_id": job_id}]
    assert redis.hget(f"job:{job_id}", "estado") == b"queued" and redis.zscore("jobs:ana", job_id)
    assert almacen.leer(redis, bucket, f"job:{job_id}:entrada:1") == png(4, 4)
    assert not redis.exists(f"job:{job_id}:reserva")
    assert cliente.get(f"/jobs/{job_id}", headers=token()).json()["contenido"]["estado"] == "queued"


@pytest.mark.parametrize("tamanos", [[validacion.ARCHIVO_MAX + 1], [validacion.ARCHIVO_MAX, validacion.ARCHIVO_MAX, 1]])
def test_los_limites_del_contrato_se_aplican_antes_de_subir_nada(cliente, bucket, worker, tamanos):
    respuesta = reservar(cliente, tamanos)
    assert respuesta.status_code == 413 and tipo(respuesta) == "ARCHIVO_DEMASIADO_GRANDE"
    assert bucket.subidas == {}


@pytest.mark.parametrize("cuerpo, campo", [
    ({"archivos": 2, "operaciones": operaciones()}, "archivos"),
    ({"archivos": [], "operaciones": operaciones()}, "archivos"),
    ({"archivos": [10, 0], "operaciones": operaciones()}, "archivos"),
    ({"archivos": [10, True], "operaciones": operaciones()}, "archivos"),
    ({"archivos": [10]}, "operaciones"),
    ({"archivos": [10], "operaciones": operaciones(), "prioridad": 1}, "prioridad"),
])
def test_una_reserva_mal_armada_es_422(cliente, worker, cuerpo, campo):
    respuesta = cliente.post("/jobs/presigned", json=cuerpo, headers=token())
    assert respuesta.status_code == 422 and tipo(respuesta) == "PAYLOAD_INVALIDO"
    assert campo in [d["campo"] for d in respuesta.json()["contenido"]["error"]["detalles"]]


def test_reservar_sin_json_es_415(cliente, worker):
    respuesta = cliente.post("/jobs/presigned", data={"archivos": "1"}, headers=token())
    assert respuesta.status_code == 415 and tipo(respuesta) == "TIPO_DE_CONTENIDO"


def test_las_operaciones_se_validan_al_reservar(cliente, bucket, worker):
    respuesta = reservar(cliente, [10], operaciones("rotar"))
    assert respuesta.status_code == 422 and tipo(respuesta) == "OPERACION_NO_SOPORTADA"
    assert bucket.subidas == {}


def test_sin_workers_no_se_reserva(cliente):
    respuesta = reservar(cliente, [10])
    assert respuesta.status_code == 503 and tipo(respuesta) == "SIN_WORKERS"


def test_confirmar_sin_haber_subido_todo_es_422_y_la_reserva_sigue(cliente, redis, rabbit, bucket, worker):
    job_id = reservar(cliente, [len(png()), len(png(4, 4))]).json()["contenido"]["job_id"]
    subir(bucket, job_id, png())  # falta el archivo 1

    respuesta = confirmar(cliente, job_id)

    assert respuesta.status_code == 422 and tipo(respuesta) == "ARCHIVO_INVALIDO"
    assert respuesta.json()["contenido"]["error"]["detalles"] == [
        {"campo": "archivos[1]", "problema": "no está en el bucket: subilo con su URL antes de confirmar"}]
    assert rabbit.colas == {} and not redis.exists(f"job:{job_id}")
    subir(bucket, job_id, png(), png(4, 4))
    assert confirmar(cliente, job_id).status_code == 202


def test_confirmar_revisa_el_tamano_y_el_formato_de_lo_subido(cliente, bucket, worker):
    declarado = len(png()) + 100
    job_id = reservar(cliente, [declarado, 9]).json()["contenido"]["job_id"]
    bucket.guardar(f"job/{job_id}/entrada/0.png", png())  # otro tamaño que el declarado
    bucket.guardar(f"job/{job_id}/entrada/1.png", b"no imagen")

    respuesta = confirmar(cliente, job_id)

    assert respuesta.status_code == 422 and tipo(respuesta) == "ARCHIVO_INVALIDO"
    assert respuesta.json()["contenido"]["error"]["detalles"] == [
        {"campo": "archivos[0]", "problema": f"pesa {len(png())} bytes y se declararon {declarado}"},
        {"campo": "archivos[1]", "problema": "no es PNG ni JPEG"}]


def encabezado_png(ancho, alto):
    """La firma, el IHDR y el comienzo de los píxeles: lo que Pillow lee para dar las dimensiones, sin armar los
    120 megapíxeles enteros."""
    def chunk(tipo, datos):
        return struct.pack(">I", len(datos)) + tipo + datos + struct.pack(">I", zlib.crc32(tipo + datos))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", ancho, alto, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(b"\x00" * 1000)))


def test_mas_de_100_megapixeles_se_detecta_con_el_encabezado(cliente, rabbit, bucket, worker):
    gigante = encabezado_png(20_000, 6_000)
    job_id = reservar(cliente, [len(gigante)]).json()["contenido"]["job_id"]
    subir(bucket, job_id, gigante)

    respuesta = confirmar(cliente, job_id)

    assert respuesta.status_code == 413 and tipo(respuesta) == "ARCHIVO_DEMASIADO_GRANDE"
    assert rabbit.colas == {}


def test_un_job_se_confirma_una_sola_vez(cliente, rabbit, bucket, worker):
    job_id = reservar(cliente, [len(png())]).json()["contenido"]["job_id"]
    subir(bucket, job_id, png())

    assert confirmar(cliente, job_id).status_code == 202
    segunda = confirmar(cliente, job_id)

    assert segunda.status_code == 404 and tipo(segunda) == "JOB_INEXISTENTE"
    assert rabbit.colas["sobel.split"] == [{"job_id": job_id}]


def test_la_reserva_de_otro_usuario_no_existe_ni_para_un_admin(cliente, bucket, worker):
    job_id = reservar(cliente, [len(png())]).json()["contenido"]["job_id"]
    subir(bucket, job_id, png())
    for otro in (token("beto"), token("root", "admin")):
        respuesta = confirmar(cliente, job_id, otro)
        assert respuesta.status_code == 404 and tipo(respuesta) == "JOB_INEXISTENTE"


def test_una_reserva_vencida_no_se_puede_confirmar(cliente, redis, bucket, worker):
    job_id = reservar(cliente, [len(png())]).json()["contenido"]["job_id"]
    subir(bucket, job_id, png())
    redis.delete(f"job:{job_id}:reserva")  # lo que hace el TTL a los 15 min
    assert confirmar(cliente, job_id).status_code == 404


def test_si_rabbitmq_no_responde_la_reserva_vuelve_y_los_archivos_quedan(cliente, redis, rabbit, bucket, worker):
    job_id = reservar(cliente, [len(png())]).json()["contenido"]["job_id"]
    subir(bucket, job_id, png())
    rabbit.caido = True

    respuesta = confirmar(cliente, job_id)

    assert respuesta.status_code == 503 and tipo(respuesta) == "SERVICIO_NO_DISPONIBLE"
    assert not redis.exists(f"job:{job_id}", f"job:{job_id}:entrada:0") and redis.zcard("jobs") == 0
    assert redis.exists(f"job:{job_id}:reserva") and bucket.objetos  # se reintenta sin volver a subir
    rabbit.caido = False
    assert confirmar(cliente, job_id).status_code == 202


def test_si_el_bucket_no_responde_al_reservar_es_503(cliente, bucket, worker):
    bucket.caido = True
    respuesta = reservar(cliente, [10])
    assert respuesta.status_code == 503 and tipo(respuesta) == "SERVICIO_NO_DISPONIBLE"


def test_la_reserva_guarda_las_operaciones_validadas(cliente, redis, worker):
    ops = operaciones("grises", "blur")
    job_id = reservar(cliente, [10], ops).json()["contenido"]["job_id"]
    assert json.loads(redis.hget(f"job:{job_id}:reserva", "operaciones")) == ops
