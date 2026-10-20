"""Píxeles de un archivo: lectura en RGB, partición en fragmentos y codificación a PNG."""

import io

import numpy as np
from PIL import Image


def leer_rgb(datos):
    """Bytes PNG o JPEG → matriz uint8 (alto, ancho, 3). Normaliza a RGB: paletas, grises y CMYK; el alfa se pierde."""
    with Image.open(io.BytesIO(datos)) as archivo:
        return np.asarray(archivo.convert("RGB"))


def dividir(matriz, partes):
    """Parte en `partes` franjas horizontales. Devuelve una lista de (franja, filas_extra_arriba, filas)."""
    limites = np.linspace(0, matriz.shape[0], partes + 1).astype(int)
    return [(matriz[desde:hasta], 0, int(hasta - desde)) for desde, hasta in zip(limites[:-1], limites[1:])]


def a_png(matriz):
    salida = io.BytesIO()
    Image.fromarray(matriz).save(salida, format="PNG")
    return salida.getvalue()
