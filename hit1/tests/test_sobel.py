import numpy as np
import pytest
from PIL import Image

import sobel


def aleatoria(alto=37, ancho=23, semilla=0):
    return np.random.default_rng(semilla).integers(0, 256, (alto, ancho), dtype=np.uint8)


def test_imagen_uniforme_no_tiene_bordes():
    assert not sobel.aplicar(np.full((5, 6), 120, np.uint8)).any()


def test_rampa_horizontal():
    # valor = 10 * columna: en el interior gx = 20 * (1 + 2 + 1) = 80; en los bordes la
    # columna repetida deja la mitad.
    rampa = np.tile(np.arange(6, dtype=np.uint8) * 10, (4, 1))
    resultado = sobel.aplicar(rampa)
    assert (resultado[:, 1:-1] == 80).all()
    assert (resultado[:, [0, -1]] == 40).all()


def test_escalon_satura_en_255():
    escalon = np.zeros((4, 6), np.uint8)
    escalon[:, 3:] = 255
    resultado = sobel.aplicar(escalon)
    assert (resultado[:, 2:4] == 255).all()
    assert not resultado[:, [0, 1, 4, 5]].any()


def test_forma_y_tipo():
    resultado = sobel.aplicar(aleatoria())
    assert resultado.shape == (37, 23) and resultado.dtype == np.uint8


@pytest.mark.parametrize("partes", [1, 2, 3, 7, 36, 37])
def test_franjas_procesadas_por_separado_dan_lo_mismo_que_la_imagen_entera(partes):
    gris = aleatoria()
    franjas = sobel.dividir(gris, partes)
    assert len(franjas) == partes
    assert sum(filas for _, _, filas in franjas) == gris.shape[0]
    unida = sobel.unir([(sobel.aplicar(f), extra, filas) for f, extra, filas in franjas])
    assert np.array_equal(unida, sobel.aplicar(gris))


@pytest.mark.parametrize("partes", [0, -1, 38])
def test_partes_fuera_de_rango(partes):
    with pytest.raises(ValueError):
        sobel.dividir(aleatoria(), partes)


def test_leer_gris_convierte_color_a_gris(tmp_path):
    ruta = tmp_path / "color.png"
    Image.new("RGB", (4, 3), (255, 0, 0)).save(ruta)
    gris = sobel.leer(str(ruta))
    assert gris.shape == (3, 4) and gris.dtype == np.uint8
    assert (gris == 76).all()  # luminancia de rojo puro en Pillow (L = 0,299 R + ...)


def test_png_ida_y_vuelta():
    gris = aleatoria()
    assert np.array_equal(sobel.leer_gris(sobel.a_png(gris)), gris)
