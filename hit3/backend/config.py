"""Configuración del backend desde variables de entorno (contrato §12)."""

import os
from dataclasses import dataclass, field

from backend.auth import LARGO_MINIMO_SECRETO

# Los arma el propio backend en su /health: un servicio con estos nombres los pisaría (y "backend" se consultaría a sí mismo).
PROPIOS = {"backend", "redis", "rabbitmq", "workers"}


@dataclass(frozen=True)
class Config:
    secreto: str
    imagenes_permitidas: frozenset
    servicios: dict = field(default_factory=dict)  # nombre -> URL de su /health
    pedidos_por_minuto: int = 120
    ttl: int = 86400
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/%2F"
    redis_url: str = "redis://localhost:6379/0"

    def __post_init__(self):
        if len(self.secreto) < LARGO_MINIMO_SECRETO:
            raise ValueError(f"SOBEL_SECRETO falta o tiene menos de {LARGO_MINIMO_SECRETO} caracteres")
        if not self.imagenes_permitidas:
            raise ValueError("SOBEL_IMAGENES_PERMITIDAS está vacía")
        for imagen in self.imagenes_permitidas:
            if not _fijada(imagen):
                raise ValueError(f"la imagen permitida {imagen} tiene que fijar un tag (no latest) o un digest")
        for nombre, url in self.servicios.items():
            if nombre in PROPIOS:
                raise ValueError(f"SOBEL_SERVICIOS no puede incluir {nombre}: el backend ya lo informa")
            if not url.endswith("/health"):
                raise ValueError(f"SOBEL_SERVICIOS: la URL de {nombre} tiene que ser la de su /health ({url})")

    @classmethod
    def desde_entorno(cls, entorno=os.environ):
        return cls(
            secreto=entorno.get("SOBEL_SECRETO", ""),
            imagenes_permitidas=frozenset(_lista(entorno.get("SOBEL_IMAGENES_PERMITIDAS", ""))),
            servicios=dict(par.split("=", 1) for par in _lista(entorno.get("SOBEL_SERVICIOS", ""))),
            pedidos_por_minuto=int(entorno.get("SOBEL_PEDIDOS_POR_MINUTO", 120)),
            ttl=int(entorno.get("SOBEL_TTL", 86400)),
            rabbitmq_url=_obligatoria(entorno, "RABBITMQ_URL"),
            redis_url=_obligatoria(entorno, "REDIS_URL"),
        )


def _obligatoria(entorno, nombre):
    """Sin default (contrato §12): si falta el Secret, mejor no arrancar que conectarse a localhost."""
    if not entorno.get(nombre):
        raise ValueError(f"falta la variable de entorno {nombre}")
    return entorno[nombre]


def _lista(texto):
    return [parte.strip() for parte in texto.split(",") if parte.strip()]


def _fijada(imagen):
    if "@sha256:" in imagen:
        return True
    nombre = imagen.rsplit("/", 1)[-1]
    return ":" in nombre and not nombre.endswith(":latest")
