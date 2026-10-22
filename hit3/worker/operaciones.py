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


OPERACIONES = {"grises": grises}
