import numpy as np
import pytest
from PIL import Image

from worker import operaciones


def imagen_rgb(alto, ancho, semilla=0):
    return np.random.default_rng(semilla).integers(0, 256, (alto, ancho, 3), dtype=np.uint8)


def test_sobel_sobre_grises_es_el_del_hit1(sobel_hit1):
    gris = imagen_rgb(9, 7)[:, :, 0]

    np.testing.assert_array_equal(operaciones.sobel(gris), sobel_hit1.aplicar(gris))


def test_sobel_sobre_rgb_primero_la_pasa_a_grises_como_el_hit1(sobel_hit1):
    rgb = imagen_rgb(9, 7)

    resultado = operaciones.sobel(rgb)

    np.testing.assert_array_equal(resultado, sobel_hit1.aplicar(np.asarray(Image.fromarray(rgb).convert("L"))))


# Calculadas a mano: el borde se completa repitiendo la fila y la columna del borde, y cada píxel es
# (suma de su ventana 3×3 + 4) // 9. En la primera, (0, 0) suma 4 × 14 = 56 → 6; (1, 1) suma 14 → 1,56 → 2.
@pytest.mark.parametrize("entrada, esperado", [
    ([[14, 0, 0], [0, 0, 0], [0, 0, 0]], [[6, 3, 0], [3, 2, 0], [0, 0, 0]]),
    ([[0, 3]], [[1, 2]]),  # una sola fila: arriba y abajo se repite ella misma; suman 9 y 18
], ids=["esquina", "una_fila"])
def test_blur_promedia_cada_pixel_con_sus_vecinos(entrada, esperado):
    resultado = operaciones.blur(np.array(entrada, dtype=np.uint8))

    np.testing.assert_array_equal(resultado, np.array(esperado, dtype=np.uint8))


def test_blur_sobre_rgb_conserva_los_colores_y_promedia_cada_canal_por_separado():
    rgb = np.zeros((3, 3, 3), dtype=np.uint8)
    rgb[:, :, 0] = 200  # una constante no cambia
    rgb[0, 0, 1] = 14   # la esquina de arriba: en el canal verde da lo mismo que en grises

    resultado = operaciones.blur(rgb)

    assert resultado.shape == (3, 3, 3)
    np.testing.assert_array_equal(resultado[:, :, 0], np.full((3, 3), 200))
    np.testing.assert_array_equal(resultado[:, :, 1], [[6, 3, 0], [3, 2, 0], [0, 0, 0]])
    np.testing.assert_array_equal(resultado[:, :, 2], np.zeros((3, 3)))


@pytest.mark.parametrize("operacion, canales_de_entrada, forma_de_salida", [
    ("grises", 3, (5, 4)), ("grises", 1, (5, 4)),
    ("sobel", 3, (5, 4)), ("sobel", 1, (5, 4)),
    ("blur", 3, (5, 4, 3)), ("blur", 1, (5, 4)),
])
def test_cada_operacion_devuelve_sus_canales_naturales(operacion, canales_de_entrada, forma_de_salida):
    entrada = imagen_rgb(5, 4) if canales_de_entrada == 3 else imagen_rgb(5, 4)[:, :, 0]

    salida = operaciones.OPERACIONES[operacion](entrada)

    assert salida.shape == forma_de_salida
    assert salida.dtype == np.uint8
