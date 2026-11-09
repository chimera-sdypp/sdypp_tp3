from backend import app as modulo_app
from conftest import IMAGEN, OTRA, enviar, token

ADMIN = token("root", "admin")


def test_stats(cliente, redis, rabbit, worker):
    for _ in range(3):
        enviar(cliente)
    redis.hset("worker:w2", mapping={"imagen": OTRA, "operaciones": "sobel"})
    rabbit.colas.update({f"sobel.tareas.{IMAGEN}": [{}] * 14, "sobel.dlq": [{}], "sobel.dlq.agotados": [{}] * 2})
    redis.lpush("terminados", "done 1000", "done 2000", "failed 500", "dlq 9000")
    contenido = cliente.get("/stats", headers=ADMIN).json()["contenido"]
    assert contenido["jobs"] == {"queued": 3, "processing": 0, "done": 0, "failed": 0, "dlq": 0}
    assert contenido["colas"] == {"sobel.split": 3, "sobel.joiner": 0, f"sobel.tareas.{IMAGEN}": 14,
                                  f"sobel.tareas.{OTRA}": 0}
    assert contenido["dlq"] == {"fallidos": 1, "agotados": 2}
    assert contenido["workers_activos"] == 2
    assert contenido["latencia_s"] == {"p50": 1.0, "p95": 9.0, "p99": 9.0, "muestras": 4}
    assert contenido["tasa_error"] == 0.5


def test_stats_sin_datos(cliente):
    contenido = cliente.get("/stats", headers=ADMIN).json()["contenido"]
    assert contenido["latencia_s"] == {"p50": None, "p95": None, "p99": None, "muestras": 0}
    assert contenido["tasa_error"] == 0.0 and contenido["workers_activos"] == 0


def test_reprocesar_la_dlq(cliente, redis, rabbit, worker):
    job_id = enviar(cliente).json()["contenido"]["job_id"]
    redis.hset(f"job:{job_id}", mapping={"estado": "dlq", "en_dlq": 2, "terminado": 1,
                                         "error": '{"tipo": "DLQ", "mensaje": "x"}'})
    tarea = {"job_id": job_id, "archivo": 0, "fragmento": 1, "paso": 0, "intento": 4,
             "operaciones": [{"operacion": "sobel", "imagen": IMAGEN}]}
    rabbit.colas["sobel.dlq.agotados"] = [dict(tarea), dict(tarea, fragmento=2)]
    respuesta = cliente.post("/admin/dlq/reprocesar", headers=ADMIN)
    assert respuesta.json()["contenido"] == {"reprocesados": 2, "jobs": [job_id]}
    assert [t["intento"] for t in rabbit.colas[f"sobel.tareas.{IMAGEN}"]] == [1, 1]
    job = redis.hgetall(f"job:{job_id}")
    assert job[b"estado"] == b"processing" and job[b"en_dlq"] == b"0"
    assert b"error" not in job and b"terminado" not in job


def test_reprocesar_no_resucita_un_job_terminado(redis):
    from backend import jobs
    redis.hset("job:j", mapping={"estado": "failed", "en_dlq": 1})
    jobs.volver_a_procesar(redis, ["j", "vencido"])
    assert redis.hget("job:j", "estado") == b"failed" and not redis.exists("job:vencido")


def test_health_resume_cada_servicio(config, redis, rabbit, bucket, worker, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from backend.config import Config
    monkeypatch.setenv("SOBEL_LOGS", str(tmp_path))
    monkeypatch.setattr(modulo_app, "_consultar", lambda url: "ok" if "split" in url else "caido")
    con_servicios = Config(secreto=config.secreto, imagenes_permitidas=config.imagenes_permitidas,
                           servicios={"split": "http://split:8080/health", "joiner": "http://joiner:8080/health"})
    cliente = TestClient(modulo_app.crear_app(con_servicios, redis, rabbit, mantener=False, almacen=bucket))
    respuesta = cliente.get("/health")
    assert respuesta.status_code == 200
    assert respuesta.json()["contenido"] == {"backend": "ok", "redis": "ok", "rabbitmq": "ok", "almacen": "ok",
                                             "split": "ok", "joiner": "caido", "workers": "ok"}


def test_health_503_si_rabbitmq_o_redis_caen(cliente, rabbit, redis, monkeypatch):
    rabbit.caido = True
    respuesta = cliente.get("/health")
    assert respuesta.status_code == 503 and respuesta.json()["contenido"]["rabbitmq"] == "caido"
    assert respuesta.json()["contenido"]["workers"] == "caido"
    rabbit.caido = False
    monkeypatch.setattr(redis, "ping", lambda: (_ for _ in ()).throw(ConnectionError()))
    assert cliente.get("/health").json()["contenido"]["redis"] == "caido"


def test_health_503_si_el_bucket_no_responde(cliente, bucket):
    bucket.caido = True
    respuesta = cliente.get("/health")
    assert respuesta.status_code == 503 and respuesta.json()["contenido"]["almacen"] == "caido"


def test_redis_caido_es_503(cliente, redis, monkeypatch):
    import redis as redis_py
    def caido(*a, **k):
        raise redis_py.exceptions.ConnectionError("sin redis")
    monkeypatch.setattr(redis, "pipeline", caido)
    respuesta = cliente.get("/jobs", headers=token())
    assert respuesta.status_code == 503
    assert respuesta.json()["contenido"]["error"]["tipo"] == "SERVICIO_NO_DISPONIBLE"


def test_error_inesperado_es_500_sin_detalle(cliente, monkeypatch):
    from backend import jobs
    monkeypatch.setattr(jobs, "listar", lambda *a, **k: 1 / 0)
    respuesta = cliente.get("/jobs", headers=token())
    assert respuesta.status_code == 500
    assert respuesta.json()["contenido"]["error"] == {"tipo": "ERROR_INTERNO", "mensaje": "error interno del servidor",
                                                      "detalles": []}


def test_metricas(cliente):
    cliente.get("/jobs", headers=token())
    cliente.get("/jobs")
    texto = cliente.get("/metrics").text
    assert 'sobel_pedidos_total{codigo="200",ruta="/jobs"}' in texto
    assert 'sobel_errores_total{servicio="backend",tipo="NO_AUTORIZADO"}' in texto


def test_config_dinamica_es_solo_para_admin(cliente):
    assert cliente.get("/admin/config", headers=token()).status_code == 403
    assert cliente.put("/admin/config", json={"max_partes": 4}, headers=token()).status_code == 403


def test_config_dinamica_se_lee_y_se_cambia_en_caliente(cliente, redis):
    inicial = cliente.get("/admin/config", headers=ADMIN).json()["contenido"]
    assert inicial["personalizadas"] == {} and inicial["config"]["mb_por_fragmento"] == 8
    assert set(inicial["perillas"]) == set(inicial["config"])

    cambiada = cliente.put("/admin/config", json={"mb_por_fragmento": 16, "png_nivel": 3}, headers=ADMIN)
    assert cambiada.status_code == 200
    assert cambiada.json()["contenido"]["personalizadas"] == {"mb_por_fragmento": 16, "png_nivel": 3}
    assert redis.hgetall("config:dinamica") == {b"mb_por_fragmento": b"16", b"png_nivel": b"3"}

    # null vuelve una perilla a su valor por defecto
    vuelta = cliente.put("/admin/config", json={"mb_por_fragmento": None}, headers=ADMIN).json()["contenido"]
    assert vuelta["config"]["mb_por_fragmento"] == 8 and vuelta["personalizadas"] == {"png_nivel": 3}


def test_config_dinamica_invalida_es_422_y_no_cambia_nada(cliente, redis):
    respuesta = cliente.put("/admin/config", json={"max_partes": 0, "inexistente": 1, "png_nivel": 2}, headers=ADMIN)
    assert respuesta.status_code == 422
    assert {d["campo"] for d in respuesta.json()["contenido"]["error"]["detalles"]} == {"max_partes", "inexistente"}
    assert not redis.exists("config:dinamica")
    assert cliente.put("/admin/config", content=b"x", headers=ADMIN).status_code == 415
