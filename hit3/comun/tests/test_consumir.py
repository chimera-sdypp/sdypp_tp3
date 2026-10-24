import json
from types import SimpleNamespace

import pika.exceptions
import pytest
import redis.exceptions
from prometheus_client import REGISTRY

from comun import conexion


class Fin(Exception):
    """La tira el canal falso cuando se queda sin mensajes, para cortar el loop de `consumir`."""


class CanalFalso:
    """Imita lo que `consumir` usa de un canal de pika y entrega `mensajes` en orden."""

    def __init__(self, mensajes, al_vaciarse=Fin, propiedades=None):
        self.mensajes = list(mensajes)
        self.al_vaciarse = al_vaciarse
        self.propiedades = propiedades
        self.acks, self.nacks = [], []
        self.eventos = []

    def confirm_delivery(self):
        self.eventos.append("confirm_delivery")

    def basic_qos(self, prefetch_count):
        self.eventos.append(f"basic_qos({prefetch_count})")

    def basic_consume(self, cola, on_message_callback):
        self.eventos.append("basic_consume")
        self.cola, self.entregar = cola, on_message_callback

    def start_consuming(self):
        for tag, cuerpo in enumerate(self.mensajes, start=1):
            self.entregar(self, SimpleNamespace(delivery_tag=tag), self.propiedades, cuerpo)
        if self.al_vaciarse:
            raise self.al_vaciarse()

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacks.append((delivery_tag, requeue))


class ConexionFalsa:
    def __init__(self, canal):
        self.canal = canal

    def channel(self):
        return self.canal


def test_ack_si_el_callback_termina_bien():
    canal = CanalFalso([b"tarea 1", b"tarea 2"])
    recibidos = []

    with pytest.raises(Fin):
        conexion.consumir("sobel.tareas.grises", lambda c, cuerpo: recibidos.append(cuerpo),
                          conectar=lambda: ConexionFalsa(canal))

    assert recibidos == [b"tarea 1", b"tarea 2"]
    assert canal.cola == "sobel.tareas.grises"
    assert canal.acks == [1, 2]
    assert canal.nacks == []


def test_con_propiedades_el_callback_recibe_tambien_las_del_mensaje():
    propiedades = SimpleNamespace(headers={"x-first-death-queue": "sobel.joiner"})
    canal = CanalFalso([b"tarea 1"], propiedades=propiedades)
    recibidos = []

    with pytest.raises(Fin):
        conexion.consumir("sobel.dlq", lambda c, cuerpo, props: recibidos.append((cuerpo, props)),
                          con_propiedades=True, conectar=lambda: ConexionFalsa(canal))

    assert recibidos == [(b"tarea 1", propiedades)]
    assert canal.acks == [1]


def test_nack_sin_reencolar_si_el_callback_falla_y_sigue_con_el_proximo():
    canal = CanalFalso([b"fragmento corrupto", b"fragmento sano"])

    def procesar(c, cuerpo):
        if cuerpo == b"fragmento corrupto":
            raise ValueError("no es un PNG")

    with pytest.raises(Fin):
        conexion.consumir("sobel.tareas.grises", procesar, conectar=lambda: ConexionFalsa(canal))

    assert canal.nacks == [(1, False)]
    assert canal.acks == [2]


def test_prepara_el_canal_antes_de_consumir():
    canal = CanalFalso([])

    with pytest.raises(Fin):
        conexion.consumir("sobel.joiner", lambda c, cuerpo: None, prefetch=4,
                          al_conectar=lambda c: c.eventos.append("al_conectar"),
                          conectar=lambda: ConexionFalsa(canal))

    assert canal.eventos == ["confirm_delivery", "basic_qos(4)", "al_conectar", "basic_consume"]


def test_si_se_cae_rabbitmq_reconecta_y_vuelve_a_preparar_el_canal():
    antes = CanalFalso([b"tarea 1"], al_vaciarse=lambda: pika.exceptions.StreamLostError("socket cerrado"))
    despues = CanalFalso([b"tarea 2"])
    conexiones = iter([ConexionFalsa(antes), ConexionFalsa(despues)])
    preparados = []

    with pytest.raises(Fin):
        conexion.consumir("sobel.split", lambda c, cuerpo: None, al_conectar=preparados.append,
                          conectar=lambda: next(conexiones))

    assert preparados == [antes, despues]
    assert antes.acks == [1]
    assert despues.acks == [1]


class RedisFalso:
    """`ping` falla `caidas` veces y después responde."""

    def __init__(self, caidas):
        self.caidas = caidas

    def ping(self):
        if self.caidas:
            self.caidas -= 1
            raise redis.exceptions.ConnectionError("redis caído")
        return True


def test_si_se_cae_redis_reencola_y_espera_a_que_vuelva():
    canal = CanalFalso([b"fragmento 1", b"fragmento 2"])
    cliente = RedisFalso(caidas=2)
    esperas = []

    def guardar(c, cuerpo):
        if cuerpo == b"fragmento 1":
            raise redis.exceptions.ConnectionError("redis caído")

    with pytest.raises(Fin):
        conexion.consumir("sobel.tareas.grises", guardar, cliente_redis=cliente, dormir=esperas.append,
                          conectar=lambda: ConexionFalsa(canal))

    assert canal.nacks == [(1, True)]
    assert esperas == [1, 2]
    assert canal.acks == [2]


def test_si_se_cae_rabbitmq_dentro_del_callback_no_manda_el_mensaje_a_la_dlq():
    antes = CanalFalso([b"tarea 1"])
    despues = CanalFalso([b"tarea 1"])
    conexiones = iter([ConexionFalsa(antes), ConexionFalsa(despues)])

    def publicar_siguiente(c, cuerpo):
        if c is antes:
            raise pika.exceptions.StreamLostError("socket cerrado al publicar")

    with pytest.raises(Fin):
        conexion.consumir("sobel.tareas.grises", publicar_siguiente, conectar=lambda: next(conexiones))

    assert antes.nacks == []
    assert despues.acks == [1]


def test_stop_consuming_termina_sin_reconectar():
    canal = CanalFalso([b"tarea 1"], al_vaciarse=None)
    conexiones = iter([ConexionFalsa(canal)])

    conexion.consumir("sobel.split", lambda c, cuerpo: None, conectar=lambda: next(conexiones))

    assert canal.acks == [1]


def test_consume_la_cola_que_devuelve_al_conectar_y_la_renueva_al_reconectar():
    antes = CanalFalso([], al_vaciarse=lambda: pika.exceptions.StreamLostError("socket cerrado"))
    despues = CanalFalso([])
    conexiones = iter([ConexionFalsa(antes), ConexionFalsa(despues)])
    nombres = iter(["amq.gen-primera", "amq.gen-segunda"])

    with pytest.raises(Fin):
        conexion.consumir(None, lambda c, cuerpo: None, al_conectar=lambda c: next(nombres),
                          conectar=lambda: next(conexiones))

    assert antes.cola == "amq.gen-primera"
    assert despues.cola == "amq.gen-segunda"


def errores(servicio, tipo):
    """El contador es global (el de Prometheus): los tests miran cuánto sumó."""
    return REGISTRY.get_sample_value("sobel_errores_total", {"servicio": servicio, "tipo": tipo}) or 0


class TiempoAgotado(Exception):
    pass


def test_cuenta_cada_nack_por_el_nombre_de_su_excepcion_y_cada_caida_de_redis():
    canal = CanalFalso([b"lento", b"redis", b"lento"])
    antes = errores("worker", "TIEMPO_AGOTADO"), errores("worker", "REDIS_CAIDO")

    def procesar(c, cuerpo):
        raise TiempoAgotado() if cuerpo == b"lento" else redis.exceptions.ConnectionError("redis caído")

    with pytest.raises(Fin):
        conexion.consumir("sobel.tareas.x", procesar, servicio="worker", cliente_redis=RedisFalso(caidas=0),
                          conectar=lambda: ConexionFalsa(canal))

    assert errores("worker", "TIEMPO_AGOTADO") - antes[0] == 2
    assert errores("worker", "REDIS_CAIDO") - antes[1] == 1


def test_el_tipo_de_un_error_con_siglas_no_las_parte_letra_por_letra():
    canal = CanalFalso([b"{roto"])
    antes = errores("split", "JSON_DECODE_ERROR")

    with pytest.raises(Fin):
        conexion.consumir("sobel.split", lambda c, cuerpo: json.loads(cuerpo), servicio="split",
                          conectar=lambda: ConexionFalsa(canal))

    assert errores("split", "JSON_DECODE_ERROR") - antes == 1
