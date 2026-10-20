"""La conexión del backend con RabbitMQ.

Una sola conexión para todos los hilos del servidor, con un candado (pika no es thread-safe).
Si RabbitMQ se cae, los pedidos no esperan: reciben 503 al instante y la reconexión se intenta
con backoff (1, 2, 4… s, tope 30 s) sin bloquear a nadie.
"""

import json
import logging
import threading
import time

import pika
import pika.exceptions

from comun import colas
from comun.conexion import ESPERA_INICIAL, ESPERA_MAXIMA, estado
from backend.jobs import ahora_ms


class NoDisponible(Exception):
    """RabbitMQ no responde."""


class Rabbit:
    def __init__(self, url, log=logging.getLogger("tp3.backend"), reloj=time.monotonic):
        self.parametros = pika.URLParameters(url)
        self.parametros.socket_timeout = 5
        self.log, self.reloj = log, reloj
        self.candado = threading.Lock()
        self.conexion = self.canal = None
        self.espera, self.proximo_intento = ESPERA_INICIAL, 0.0

    def publicar(self, cola, mensaje):
        self._usar(lambda canal: colas.publicar(canal, cola, mensaje))

    def contar(self, imagenes):
        """Mensajes en cada cola: las fijas y la de cada imagen permitida."""
        def contar_todas(canal):
            cuentas = {cola: canal.queue_declare(cola, passive=True).method.message_count
                       for cola in (colas.SPLIT, colas.JOINER, colas.DLQ, colas.AGOTADOS)}
            for imagen in sorted(imagenes):
                cola = colas.declarar_tareas(canal, imagen)  # puede no existir todavía: un passive cerraría el canal
                cuentas[cola] = canal.queue_declare(cola, passive=True).method.message_count
            return cuentas
        return self._usar(contar_todas)

    def reprocesar_agotados(self):
        """Pasa todo lo de sobel.dlq.agotados a la cola de su imagen, con intento 1. Devuelve los job_id."""
        def mover(canal):
            movidos = []
            while True:
                metodo, _, cuerpo = canal.basic_get(colas.AGOTADOS)
                if metodo is None:
                    return movidos
                try:
                    tarea = json.loads(cuerpo)
                    imagen = tarea["operaciones"][tarea["paso"]]["imagen"]
                except (ValueError, KeyError, IndexError, TypeError):
                    self.log.error("mensaje ilegible en %s, lo descarto: %r", colas.AGOTADOS, cuerpo[:200])
                    canal.basic_nack(metodo.delivery_tag, requeue=False)
                    continue
                tarea.update(intento=1, encolado=ahora_ms())
                colas.publicar(canal, colas.declarar_tareas(canal, imagen), tarea)
                canal.basic_ack(metodo.delivery_tag)
                movidos.append(tarea["job_id"])
        return self._usar(mover)

    def vivo(self):
        try:
            self._usar(lambda canal: self.conexion.process_data_events(0))
            return True
        except NoDisponible:
            return False

    def _usar(self, accion):
        with self.candado:
            try:
                return accion(self._canal())
            except NoDisponible:
                raise
            except (pika.exceptions.AMQPError, OSError) as error:
                self.log.warning("se cortó RabbitMQ (%r)", error)
                self._cerrar()
                raise NoDisponible() from error

    def _canal(self):
        if self.canal is not None and self.canal.is_open:
            return self.canal
        if self.reloj() < self.proximo_intento:
            raise NoDisponible()
        try:
            self.conexion = pika.BlockingConnection(self.parametros)
            canal = self.conexion.channel()
            canal.confirm_delivery()
            colas.declarar(canal)
        except (pika.exceptions.AMQPError, OSError) as error:
            self._cerrar()
            self.proximo_intento = self.reloj() + self.espera
            self.log.warning("no conecta a RabbitMQ (%r): próximo intento en %d s", error, self.espera)
            self.espera = min(self.espera * 2, ESPERA_MAXIMA)
            raise NoDisponible() from error
        self.log.info("conectado a RabbitMQ")
        self.canal, self.espera = canal, ESPERA_INICIAL
        estado["rabbitmq"] = "ok"
        return canal

    def _cerrar(self):
        estado["rabbitmq"] = "caido"
        try:
            if self.conexion is not None and self.conexion.is_open:
                self.conexion.close()
        except Exception:
            pass
        self.conexion = self.canal = None
