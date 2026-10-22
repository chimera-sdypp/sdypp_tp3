"""Operaciones del worker: funciones puras de matriz uint8 a matriz uint8.

Cada una devuelve los canales naturales de su resultado (alto × ancho para un canal, alto × ancho × 3 para RGB),
y la salida de un paso es la entrada del siguiente.
"""

import numpy as np
from PIL import Image


def grises(matriz):
    """RGB → un canal, con la conversión `L` de Pillow (la misma del Hit 1). Un canal queda igual."""
    if matriz.ndim == 2:
        return matriz
    return np.asarray(Image.fromarray(matriz).convert("L"))


# Kernels del Sobel del Hit 1 (hit1/sobel.py). Se copia en vez de importarlo: las imágenes se construyen con
# contexto hit3/, que no incluye hit1/.
GX = np.array([[-1, 0, 1],
               [-2, 0, 2],
               [-1, 0, 1]])
GY = GX.T


def sobel(matriz):
    """Magnitud del gradiente, igual que el Hit 1: bordes repitiendo la fila/columna del borde y recorte a 0-255.

    Así cada píxel depende sólo de sus 8 vecinos y un fragmento con una fila de borde da lo mismo que la imagen entera.
    Si llega en RGB, primero la pasa a grises, como el Hit 1.
    """
    gris = grises(matriz)
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


def blur(matriz):
    """Promedio 3×3 (box blur), con los bordes repitiendo la fila/columna del borde y redondeo entero.

    Un kernel 3×3 necesita una sola fila de borde por lado, que es lo que agrega el split (contrato §10).
    En RGB promedia cada canal por separado y conserva los colores.
    """
    alto, ancho = matriz.shape[:2]
    bordes = [(1, 1), (1, 1)] + [(0, 0)] * (matriz.ndim - 2)  # los canales no se completan
    p = np.pad(matriz.astype(np.int32), bordes, mode="edge")
    suma = sum(p[i:i + alto, j:j + ancho] for i in range(3) for j in range(3))
    return ((suma + 4) // 9).astype(np.uint8)


OPERACIONES = {"grises": grises, "sobel": sobel, "blur": blur}
