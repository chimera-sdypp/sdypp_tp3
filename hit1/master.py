"""Etapas 2 y 3 — master: divide la imagen en N franjas, las reparte entre los workers y une el resultado.

    python master.py entrada.jpg salida.png --workers worker:8080 --partes 4

Cada host de --workers se resuelve por DNS y se toman todas sus direcciones: en Docker
Compose `worker` devuelve una IP por réplica, así que `--scale worker=N` da N workers.

Etapa 3: si un worker se cae (conexión rechazada o cortada, 5xx) o no responde dentro de
--timeout, se lo descarta y su franja se reasigna a otro worker vivo. Si no queda ninguno,
se vuelve a resolver el DNS por si aparecieron workers nuevos (p. ej. el pod que k8s
recreó) antes de abandonar el trabajo.
"""

import argparse
import http.client
import itertools
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import registro
import sobel

log, _ = registro.configurar("master")


class ErrorWorker(Exception):
    """No quedó ningún worker disponible para terminar el trabajo."""


class WorkerCaido(Exception):
    """El worker se cayó o no respondió: la franja se puede reasignar a otro."""


class ErrorFranja(ValueError):
    """El worker rechazó la franja (4xx): reasignarla no lo arregla."""


def resolver_workers(direcciones):
    """'worker:8080,otro:8080' → ['172.18.0.3:8080', '172.18.0.4:8080', ...] sin repetidos."""
    workers = []
    for direccion in direcciones.split(","):
        host, _, puerto = direccion.strip().rpartition(":")
        if not host or not puerto.isdigit():
            raise ValueError(f"dirección de worker inválida: {direccion!r} (se espera host:puerto)")
        try:
            infos = socket.getaddrinfo(host, int(puerto), socket.AF_INET, socket.SOCK_STREAM)
        except socket.gaierror as error:
            raise ValueError(f"no se pudo resolver el worker {host}: {error}") from None
        for info in infos:
            workers.append(f"{info[4][0]}:{puerto}")
    return sorted(set(workers))


def enviar(worker, franja, timeout):
    """Manda la franja (PNG) a POST /sobel del worker y devuelve el resultado como matriz.

    Lanza WorkerCaido si el worker no está, se corta, no responde en `timeout` segundos,
    contesta 5xx o devuelve algo que no es una imagen; ErrorFranja si contesta 4xx.
    """
    pedido = urllib.request.Request(f"http://{worker}/sobel", data=sobel.a_png(franja),
                                    headers={"Content-Type": "image/png"})
    try:
        with urllib.request.urlopen(pedido, timeout=timeout) as respuesta:
            datos = respuesta.read()
    except urllib.error.HTTPError as error:  # antes que OSError: HTTPError también lo es
        if error.code >= 500:
            raise WorkerCaido(f"el worker {worker} respondió {error.code}") from None
        raise ErrorFranja(f"el worker {worker} rechazó la franja: {error.code} {error.reason}") from None
    except OSError as error:  # URLError, conexión rechazada/cortada y timeout son OSError
        motivo = getattr(error, "reason", error)
        if isinstance(motivo, TimeoutError):
            raise WorkerCaido(f"el worker {worker} no respondió en {timeout:g} s") from None
        raise WorkerCaido(f"el worker {worker} no está disponible: {motivo}") from None
    except http.client.HTTPException as error:  # después de OSError: RemoteDisconnected es las dos cosas
        raise WorkerCaido(f"el worker {worker} cortó la respuesta: {error!r}") from None
    try:
        return sobel.leer_gris(datos)
    except ValueError:
        raise WorkerCaido(f"el worker {worker} devolvió algo que no es una imagen") from None


class Workers:
    """Workers de un trabajo. Los que fallan se descartan hasta el final del trabajo.

    `elegir(i)` respeta el round-robin de la Etapa 2 (franja i → worker i % N) mientras ese
    worker esté vivo; si no, rota entre los vivos. Es seguro usarlo desde varios hilos.
    """

    def __init__(self, direcciones, redescubrir=None):
        self.direcciones = list(direcciones)
        self.caidos = set()
        self.reasignaciones = 0
        self._redescubrir = redescubrir
        self._turno = itertools.count()
        self._lock = threading.Lock()

    def vivos(self):
        return [w for w in self.direcciones if w not in self.caidos]

    def elegir(self, indice):
        with self._lock:
            preferido = self.direcciones[indice % len(self.direcciones)]
            if preferido not in self.caidos:
                return preferido
            vivos = self.vivos() or self._buscar_nuevos()
            if not vivos:
                raise ErrorWorker(f"no quedan workers disponibles (caídos: {', '.join(sorted(self.caidos))})")
            return vivos[next(self._turno) % len(vivos)]

    def descartar(self, worker):
        with self._lock:
            self.caidos.add(worker)
            self.reasignaciones += 1

    def _buscar_nuevos(self):
        """Vuelve a resolver el DNS y suma los workers que no se conocían."""
        if self._redescubrir is None:
            return []
        try:
            nuevos = [w for w in self._redescubrir() if w not in self.direcciones]
        except ValueError as error:
            log.warning("no se pudieron redescubrir workers: %s", error)
            return []
        if nuevos:
            log.info("workers nuevos encontrados por DNS: %s", ", ".join(nuevos))
            self.direcciones.extend(nuevos)
        return nuevos


def procesar(gris, workers, partes, timeout, redescubrir=None):
    """Reparte las franjas en round-robin, una por hilo, y devuelve la imagen unida.

    Si el worker de una franja se cae o no responde, se lo descarta y la franja se reintenta
    en otro. Lanza ErrorWorker sólo si no queda ningún worker.
    """
    franjas = sobel.dividir(gris, partes)
    disponibles = Workers(workers, redescubrir)

    def tarea(indice):
        franja, extra, filas = franjas[indice]
        intento = 1
        while True:
            worker = disponibles.elegir(indice)
            inicio = time.perf_counter()
            try:
                resultado = enviar(worker, franja, timeout)
            except WorkerCaido as error:
                disponibles.descartar(worker)
                log.warning("franja %d/%d: %s → worker descartado, la franja se reasigna", indice + 1, partes, error)
                intento += 1
                continue
            reintento = f" (intento {intento})" if intento > 1 else ""
            log.info("franja %d/%d (%d filas) → %s en %.3f s%s", indice + 1, partes, filas, worker,
                     time.perf_counter() - inicio, reintento)
            return resultado, extra, filas

    with ThreadPoolExecutor(max_workers=partes) as pool:
        resultados = list(pool.map(tarea, range(partes)))
    if disponibles.caidos:
        log.warning("%d reasignaciones; workers descartados: %s", disponibles.reasignaciones,
                    ", ".join(sorted(disponibles.caidos)))
    return sobel.unir(resultados)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Aplica Sobel repartiendo la imagen entre N workers.")
    parser.add_argument("entrada", help="imagen de entrada ('-' = stdin)")
    parser.add_argument("salida", help="imagen de salida; el formato sale de la extensión ('-' = PNG por stdout)")
    parser.add_argument("--workers", default=os.environ.get("SOBEL_WORKERS", "worker:8080"),
                        help="host:puerto separados por coma (default: $SOBEL_WORKERS o worker:8080)")
    parser.add_argument("--partes", type=int, default=os.environ.get("SOBEL_PARTES") or None,
                        help="cantidad de franjas (default: $SOBEL_PARTES o una por worker)")
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("SOBEL_TIMEOUT", 30)),
                        help="segundos sin respuesta tras los que un worker se da por caído y su franja "
                             "se reasigna (default: $SOBEL_TIMEOUT o 30)")
    args = parser.parse_args(argv)

    try:
        workers = resolver_workers(args.workers)
        partes = len(workers) if args.partes is None else args.partes
        inicio = time.perf_counter()
        gris = sobel.leer(args.entrada)
        log.info("imagen %dx%d → %d franjas entre %d workers: %s", gris.shape[1], gris.shape[0],
                 partes, len(workers), ", ".join(workers))
        leida = time.perf_counter()
        resultado = procesar(gris, workers, partes, args.timeout,
                             redescubrir=lambda: resolver_workers(args.workers))
        procesada = time.perf_counter()
        sobel.guardar(resultado, args.salida)
    except ErrorWorker as error:
        log.error("no se pudo procesar %s: %s", args.entrada, error)
        return 1
    except (OSError, ValueError) as error:
        log.error("no se pudo procesar %s: %s", args.entrada, error)
        return 2
    fin = time.perf_counter()
    log.info("lectura %.3f s | reparto y sobel %.3f s | escritura %.3f s | total %.3f s",
             leida - inicio, procesada - leida, fin - procesada, fin - inicio)
    return 0


if __name__ == "__main__":
    sys.exit(main())
