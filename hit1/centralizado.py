"""Etapa 1 — Sobel centralizado: un solo proceso lee la imagen, aplica la máscara y la guarda.

    python centralizado.py entrada.jpg salida.png
"""

import argparse
import sys
import time

import registro
import sobel


def main(argv=None):
    parser = argparse.ArgumentParser(description="Aplica Sobel a una imagen en un solo proceso.")
    parser.add_argument("entrada", help="imagen de entrada ('-' = stdin)")
    parser.add_argument("salida", help="imagen de salida; el formato sale de la extensión ('-' = PNG por stdout)")
    args = parser.parse_args(argv)
    log, _ = registro.configurar("centralizado")

    try:
        inicio = time.perf_counter()
        gris = sobel.leer(args.entrada)
        leida = time.perf_counter()
        resultado = sobel.aplicar(gris)
        procesada = time.perf_counter()
        sobel.guardar(resultado, args.salida)
    except (OSError, ValueError) as error:
        log.error("no se pudo procesar %s: %s", args.entrada, error)
        return 2
    fin = time.perf_counter()
    log.info("%dx%d px | lectura %.3f s | sobel %.3f s | escritura %.3f s | total %.3f s",
             gris.shape[1], gris.shape[0], leida - inicio, procesada - leida,
             fin - procesada, fin - inicio)
    return 0


if __name__ == "__main__":
    sys.exit(main())
