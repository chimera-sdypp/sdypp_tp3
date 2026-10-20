"""Operador de Sobel y partición de la imagen en franjas.

Lo usan las dos etapas: la centralizada aplica `aplicar` a la imagen entera y la
distribuida la parte con `dividir`, cada worker aplica `aplicar` a su franja y el
master junta los recortes con `unir`. El resultado es idéntico píxel a píxel.
"""

import io
import sys

import numpy as np
from PIL import Image, UnidentifiedImageError

GX = np.array([[-1, 0, 1],
               [-2, 0, 2],
               [-1, 0, 1]])
GY = GX.T


def aplicar(gris):
    """Devuelve la magnitud del gradiente de una imagen en escala de grises (uint8).

    El borde se completa repitiendo la fila/columna del borde y la magnitud se recorta
    a 0-255 en vez de normalizarla por el máximo: así cada píxel depende sólo de sus
    8 vecinos y una franja procesada aparte da lo mismo que la imagen entera.
    """
    alto, ancho = gris.shape
    p = np.pad(gris.astype(np.int32), 1, mode="edge")
    gx = np.zeros((alto, ancho), np.int32)
    gy = np.zeros((alto, ancho), np.int32)
    for i in range(3):
        for j in range(3):
            ventana = p[i:i + alto, j:j + ancho]
            gx += GX[i, j] * ventana
            gy += GY[i, j] * ventana
    return np.clip(np.hypot(gx, gy), 0, 255).astype(np.uint8)


def dividir(gris, partes):
    """Parte la imagen en `partes` franjas horizontales.

    Cada franja lleva una fila extra arriba y abajo (si existe) porque Sobel necesita
    los vecinos. Devuelve una lista de (franja_con_bordes, filas_extra_arriba, filas).
    """
    alto = gris.shape[0]
    if not 1 <= partes <= alto:
        raise ValueError(f"la cantidad de partes tiene que estar entre 1 y {alto} (alto de la imagen)")
    limites = np.linspace(0, alto, partes + 1).astype(int)
    franjas = []
    for desde, hasta in zip(limites[:-1], limites[1:]):
        arriba = max(desde - 1, 0)
        franjas.append((gris[arriba:min(hasta + 1, alto)], int(desde - arriba), int(hasta - desde)))
    return franjas


def unir(resultados):
    """Junta las franjas procesadas: lista de (resultado, filas_extra_arriba, filas)."""
    return np.vstack([r[extra:extra + filas] for r, extra, filas in resultados])


def leer_gris(datos):
    """Bytes de cualquier formato que lea Pillow → matriz uint8 en escala de grises."""
    try:
        with Image.open(io.BytesIO(datos)) as imagen:
            return np.asarray(imagen.convert("L"))
    except UnidentifiedImageError:
        raise ValueError("el archivo no es una imagen") from None


def a_png(matriz):
    salida = io.BytesIO()
    Image.fromarray(matriz).save(salida, format="PNG")
    return salida.getvalue()


def guardar(matriz, destino):
    """Escribe la imagen en `destino` (el formato sale de la extensión) o en stdout si es '-'."""
    if destino == "-":
        sys.stdout.buffer.write(a_png(matriz))
        sys.stdout.buffer.flush()
    else:
        Image.fromarray(matriz).save(destino)


def leer(origen):
    """Lee la imagen de un archivo o de stdin si es '-'."""
    if origen == "-":
        return leer_gris(sys.stdin.buffer.read())
    with open(origen, "rb") as archivo:
        return leer_gris(archivo.read())
