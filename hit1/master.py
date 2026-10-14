"""Etapa 2 — master: divide la imagen en N franjas, las reparte entre los workers y une el resultado.

    python master.py entrada.jpg salida.png --workers worker:8080 --partes 4

Cada host de --workers se resuelve por DNS y se toman todas sus direcciones: en Docker
Compose `worker` devuelve una IP por réplica, así que `--scale worker=N` da N workers.
"""

import argparse
import os
import socket
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import registro
import sobel

log, _ = registro.configurar("master")


class ErrorWorker(Exception):
    pass


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
    """Manda la franja (PNG) a POST /sobel del worker y devuelve el resultado como matriz."""
    pedido = urllib.request.Request(f"http://{worker}/sobel", data=sobel.a_png(franja),
                                    headers={"Content-Type": "image/png"})
    try:
        with urllib.request.urlopen(pedido, timeout=timeout) as respuesta:
            return sobel.leer_gris(respuesta.read())
    except (OSError, ValueError) as error:  # URLError y timeout son OSError
        raise ErrorWorker(f"el worker {worker} falló: {error}") from None


def procesar(gris, workers, partes, timeout):
    """Reparte las franjas en round-robin, una por hilo, y devuelve la imagen unida."""
    franjas = sobel.dividir(gris, partes)

    def tarea(indice):
        franja, extra, filas = franjas[indice]
        worker = workers[indice % len(workers)]
        inicio = time.perf_counter()
        resultado = enviar(worker, franja, timeout)
        log.info("franja %d/%d (%d filas) → %s en %.3f s", indice + 1, partes, filas, worker,
                 time.perf_counter() - inicio)
        return resultado, extra, filas

    with ThreadPoolExecutor(max_workers=partes) as pool:
        resultados = list(pool.map(tarea, range(partes)))
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
                        help="segundos de espera por franja (default: $SOBEL_TIMEOUT o 30)")
    args = parser.parse_args(argv)

    try:
        workers = resolver_workers(args.workers)
        partes = len(workers) if args.partes is None else args.partes
        inicio = time.perf_counter()
        gris = sobel.leer(args.entrada)
        log.info("imagen %dx%d → %d franjas entre %d workers: %s", gris.shape[1], gris.shape[0],
                 partes, len(workers), ", ".join(workers))
        leida = time.perf_counter()
        resultado = procesar(gris, workers, partes, args.timeout)
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
