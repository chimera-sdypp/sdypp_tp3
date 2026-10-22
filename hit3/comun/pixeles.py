"""Píxeles de un archivo: lectura en RGB, partición en fragmentos y codificación a PNG."""

import io

import numpy as np
from PIL import Image

# El contrato acepta hasta 100 megapíxeles por archivo (§2). Pillow avisa desde ~89 por defecto (protección contra
# "decompression bombs") y corta en el doble: con esto avisa recién por encima de 100.
Image.MAX_IMAGE_PIXELS = 100_000_000


def leer_rgb(datos):
    """Bytes PNG o JPEG → matriz uint8 (alto, ancho, 3). Normaliza a RGB: paletas, grises y CMYK; el alfa se pierde.

    Lanza ValueError si los bytes no son una imagen o están cortados.
    """
    try:
        with Image.open(io.BytesIO(datos)) as archivo:
            return np.asarray(archivo.convert("RGB"))
    # OSError incluye UnidentifiedImageError e "image file is truncated"; DecompressionBombError salta
    # por encima del doble de MAX_IMAGE_PIXELS (el backend ya rechaza más de 100 MP, pero por las dudas).
    except (OSError, Image.DecompressionBombError):
        raise ValueError("no se pudo decodificar") from None


def leer(datos):
    """Bytes PNG → matriz uint8 con los canales que tenga: alto × ancho para un canal, alto × ancho × 3 para RGB.

    A diferencia de `leer_rgb`, no normaliza: el worker recibe lo que dejó el paso anterior.
    Lanza ValueError si los bytes no son una imagen o están cortados.
    """
    try:
        with Image.open(io.BytesIO(datos)) as imagen:
            return np.asarray(imagen)
    except (OSError, Image.DecompressionBombError):  # los mismos casos que en leer_rgb
        raise ValueError("no se pudo decodificar") from None


def dividir(matriz, partes, filas_de_borde):
    """Parte en `partes` franjas horizontales con `filas_de_borde` filas extra arriba y abajo (si existen).

    Si la imagen tiene menos filas que `partes`, da una franja por fila (contrato §12, SOBEL_PARTES).
    Devuelve una lista de (franja_con_bordes, filas_extra_arriba, filas).
    """
    alto = matriz.shape[0]
    limites = np.linspace(0, alto, min(partes, alto) + 1).astype(int)
    franjas = []
    for desde, hasta in zip(limites[:-1], limites[1:]):
        arriba = max(desde - filas_de_borde, 0)
        franjas.append((matriz[arriba:min(hasta + filas_de_borde, alto)], int(desde - arriba), int(hasta - desde)))
    return franjas


def geometria(fragmentos):
    """Lo que devuelve `dividir` → el hash `job:<id>:geometria:<a>` (contrato §10).

    `total` y, por fragmento, `"<filas_extra_arriba>,<filas>"`: lo que el joiner necesita para recortar los bordes.
    """
    hash_geometria = {"total": len(fragmentos)}
    for indice, (_, extra, filas) in enumerate(fragmentos):
        hash_geometria[indice] = f"{extra},{filas}"
    return hash_geometria


def leer_geometria(hash_geometria):
    """El hash de `geometria` (tal como vuelve de Redis) → [(filas_extra_arriba, filas)] en el orden de los fragmentos."""
    texto = {_texto(clave): _texto(valor) for clave, valor in hash_geometria.items()}
    return [tuple(map(int, texto[str(indice)].split(","))) for indice in range(int(texto["total"]))]


def _texto(valor):
    return valor.decode() if isinstance(valor, bytes) else str(valor)


def unir(franjas, geometria_de_franjas):
    """Recorta los bordes de cada franja según su (filas_extra_arriba, filas) y las apila en orden."""
    return np.vstack([franja[extra:extra + filas] for franja, (extra, filas) in zip(franjas, geometria_de_franjas)])


def a_png(matriz):
    salida = io.BytesIO()
    Image.fromarray(matriz).save(salida, format="PNG")
    return salida.getvalue()
