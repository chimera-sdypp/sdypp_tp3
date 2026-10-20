"""Logs en memoria y en disco.

Cada línea sale por stderr (la ve `docker logs` y no ensucia la imagen que el master
escribe por stdout), a un buffer circular en RAM y a un archivo rotativo en disco.
"""

import logging
import os
from collections import deque
from logging.handlers import RotatingFileHandler
from pathlib import Path


class HandlerEnMemoria(logging.Handler):
    """Conserva las últimas `capacidad` líneas ya formateadas."""

    def __init__(self, capacidad=500):
        super().__init__()
        self.registros = deque(maxlen=capacidad)

    def emit(self, record):
        self.registros.append(self.format(record))


def configurar(nombre):
    """Devuelve (logger, handler_en_memoria); el archivo va a $SOBEL_LOGS/<nombre>.log."""
    directorio = Path(os.environ.get("SOBEL_LOGS", "logs"))
    logger = logging.getLogger(f"tp3.{nombre}")
    logger.setLevel(logging.INFO)
    for handler in logger.handlers:
        handler.close()
    logger.handlers.clear()
    logger.propagate = False

    directorio.mkdir(parents=True, exist_ok=True)
    memoria = HandlerEnMemoria()
    disco = RotatingFileHandler(directorio / f"{nombre}.log", maxBytes=1024 * 1024,
                                backupCount=3, encoding="utf-8")
    formato = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    for handler in (logging.StreamHandler(), memoria, disco):
        handler.setFormatter(formato)
        logger.addHandler(handler)
    return logger, memoria
