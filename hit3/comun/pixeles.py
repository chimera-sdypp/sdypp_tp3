"""Píxeles de un archivo: lectura en RGB, partición en fragmentos y codificación a PNG."""

import io

import numpy as np
from PIL import Image


def leer_rgb(datos):
    """Bytes PNG o JPEG → matriz uint8 (alto, ancho, 3). Normaliza a RGB: paletas, grises y CMYK; el alfa se pierde."""
    with Image.open(io.BytesIO(datos)) as archivo:
        return np.asarray(archivo.convert("RGB"))


def dividir(matriz, partes, bordes=0):
    """Parte en `partes` franjas horizontales con `bordes` filas extra arriba y abajo (si existen).

    Si la imagen tiene menos filas que `partes`, da una franja por fila (contrato §12, SOBEL_PARTES).
    Devuelve una lista de (franja_con_bordes, filas_extra_arriba, filas).
    """
    alto = matriz.shape[0]
    limites = np.linspace(0, alto, min(partes, alto) + 1).astype(int)
    franjas = []
    for desde, hasta in zip(limites[:-1], limites[1:]):
        arriba = max(desde - bordes, 0)
        franjas.append((matriz[arriba:min(hasta + bordes, alto)], int(desde - arriba), int(hasta - desde)))
    return franjas


def a_png(matriz):
    salida = io.BytesIO()
    Image.fromarray(matriz).save(salida, format="PNG")
    return salida.getvalue()
