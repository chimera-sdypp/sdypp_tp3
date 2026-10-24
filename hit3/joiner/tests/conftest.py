import importlib.util
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="session")
def sobel_hit1():
    """El Sobel centralizado del Hit 1 es la referencia; vive fuera de hit3/, así que se carga por ruta."""
    spec = importlib.util.spec_from_file_location("sobel_hit1", RAIZ / "hit1" / "sobel.py")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo
