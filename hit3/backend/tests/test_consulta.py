import json

from backend import jobs
from conftest import IMAGEN, enviar, pasos, png, token


def crear(cliente, usuario="ana", **kwargs):
    return enviar(cliente, cabeceras=token(usuario), **kwargs).json()["contenido"]["job_id"]


def ver(cliente, job_id, usuario="ana", rol="usuario"):
    return cliente.get(f"/jobs/{job_id}", headers=token(usuario, rol))


def test_job_recien_creado(cliente, worker):
    job_id = crear(cliente, operaciones=pasos("grises", "sobel"))
    contenido = ver(cliente, job_id).json()["contenido"]
    assert contenido["estado"] == "queued" and contenido["usuario"] == "ana"
    assert contenido["operaciones"] == [{"operacion": "grises", "imagen": IMAGEN},
                                        {"operacion": "sobel", "imagen": IMAGEN}]
    assert contenido["fragmentos"] == {"total": 0, "completos": 0, "porcentaje": 0.0, "en_dlq": 0}
    assert contenido["tiempos_s"] == {"split": None, "cola": None, "procesamiento": None, "join": None, "total": None}
    assert contenido["workers"] == [] and contenido["resultados"] == [] and contenido["error"] is None
    assert contenido["creado"].endswith("Z")


def test_job_en_proceso_con_lo_que_escribieron_los_otros_servicios(cliente, redis, worker):
    job_id = crear(cliente)
    redis.hset(f"job:{job_id}", mapping={"estado": "processing", "fragmentos_total": 8, "completos": 5,
                                         "t_split_ms": 210, "cola_ms": 120, "cola_n": 3, "proc_ms": 900, "proc_n": 3,
                                         "reintentos": 1})
    redis.sadd(f"job:{job_id}:workers", "w-b", "w-a")
    contenido = ver(cliente, job_id).json()["contenido"]
    assert contenido["fragmentos"]["porcentaje"] == 62.5
    assert contenido["tiempos_s"] == {"split": 0.21, "cola": 0.04, "procesamiento": 0.3, "join": None, "total": None}
    assert contenido["workers"] == ["w-a", "w-b"] and contenido["reintentos"] == 1


def test_job_terminado_y_su_resultado(cliente, redis, worker):
    job_id = crear(cliente, archivos=[png(), png()])
    creado = int(redis.hget(f"job:{job_id}", "creado"))
    redis.hset(f"job:{job_id}", mapping={"estado": "done", "terminado": creado + 4500, "t_join_ms": 80})
    redis.set(f"job:{job_id}:resultado:1", b"PNG-1")
    contenido = ver(cliente, job_id).json()["contenido"]
    assert contenido["resultados"] == [f"/jobs/{job_id}/resultado/0", f"/jobs/{job_id}/resultado/1"]
    assert contenido["tiempos_s"]["total"] == 4.5 and contenido["tiempos_s"]["join"] == 0.08
    respuesta = cliente.get(f"/jobs/{job_id}/resultado/1", headers=token())
    assert respuesta.status_code == 200 and respuesta.content == b"PNG-1"
    assert respuesta.headers["content-type"] == "image/png"
    assert cliente.get(f"/jobs/{job_id}/resultado/2", headers=token()).status_code == 404
    assert cliente.get(f"/jobs/{job_id}/resultado/x", headers=token()).status_code == 404


def test_resultado_de_un_job_sin_terminar_es_409(cliente, redis, worker):
    job_id = crear(cliente)
    redis.hset(f"job:{job_id}", mapping={"estado": "processing", "fragmentos_total": 4, "completos": 1})
    respuesta = cliente.get(f"/jobs/{job_id}/resultado/0", headers=token())
    assert respuesta.status_code == 409
    assert respuesta.json()["contenido"]["error"]["mensaje"] == "el job está en processing (25.0 %)"


def test_job_fallido_muestra_el_error(cliente, redis, worker):
    job_id = crear(cliente)
    redis.hset(f"job:{job_id}", mapping={"estado": "failed",
                                         "error": json.dumps({"tipo": "ARCHIVO_INVALIDO", "mensaje": "no decodifica"})})
    assert ver(cliente, job_id).json()["contenido"]["error"]["tipo"] == "ARCHIVO_INVALIDO"


def test_job_de_otro_usuario_es_404_y_el_admin_lo_ve(cliente, worker):
    job_id = crear(cliente, "ana")
    respuesta = ver(cliente, job_id, "beto")
    assert respuesta.status_code == 404
    assert respuesta.json()["contenido"]["error"]["tipo"] == "JOB_INEXISTENTE"
    assert cliente.get(f"/jobs/{job_id}/resultado/0", headers=token("beto")).status_code == 404
    assert ver(cliente, job_id, "admin", "admin").status_code == 200
    assert ver(cliente, "no-existe").status_code == 404


def test_listar_los_propios_y_admin_todos(cliente, redis, worker):
    a1, a2 = crear(cliente, "ana"), crear(cliente, "ana")
    b1 = crear(cliente, "beto")
    redis.hset(f"job:{a1}", "estado", "done")
    ids = lambda r: [j["job_id"] for j in r.json()["contenido"]["jobs"]]
    assert ids(cliente.get("/jobs", headers=token("ana"))) == [a2, a1]
    assert ids(cliente.get("/jobs?usuario=beto", headers=token("ana"))) == [a2, a1]  # un usuario no filtra por otro
    assert set(ids(cliente.get("/jobs", headers=token("root", "admin")))) == {a1, a2, b1}
    assert ids(cliente.get("/jobs?usuario=beto", headers=token("root", "admin"))) == [b1]
    assert ids(cliente.get("/jobs?estado=done", headers=token("ana"))) == [a1]
    assert cliente.get("/jobs?estado=raro", headers=token("ana")).status_code == 422


def test_listar_saltea_los_vencidos(redis):
    redis.zadd("jobs", {"viejo": 1, "vigente": jobs.ahora_ms()})
    redis.hset("job:vigente", mapping={"usuario": "ana", "estado": "queued", "creado": jobs.ahora_ms()})
    assert [j["job_id"] for j in jobs.listar(redis, 86400)] == ["vigente"]
    assert redis.zscore("jobs", "viejo") is None


def test_ruta_y_metodo_inexistentes(cliente):
    respuesta = cliente.get("/no-existe", headers=token())
    assert respuesta.status_code == 404 and respuesta.json()["contenido"]["error"]["tipo"] == "RUTA_INEXISTENTE"
    respuesta = cliente.delete("/jobs", headers=token())
    assert respuesta.status_code == 405 and respuesta.headers["allow"] == "GET, POST"
