"""Validación de POST /jobs antes de encolar (R2, contrato §2)."""

import io
import json

from PIL import Image

from backend.errores import ErrorApi

ARCHIVOS_MAX = 50
PASOS_MAX = 3
ARCHIVO_MAX = 100 * 1024 * 1024
PIXELES_MAX = 100_000_000
PEDIDO_MAX = 200 * 1024 * 1024
CAMPOS = {"archivos", "operaciones"}

# El límite de píxeles lo controlamos nosotros (PIXELES_MAX); el de Pillow cortaría antes con otro error.
Image.MAX_IMAGE_PIXELS = None


def campos(formulario):
    """Revisa la forma del pedido. Devuelve (archivos, operaciones) o lanza 422 con todos los problemas."""
    detalles = [{"campo": clave, "problema": "campo no permitido"} for clave in formulario if clave not in CAMPOS]
    archivos = formulario.getlist("archivos")
    if not archivos:
        detalles.append({"campo": "archivos", "problema": "falta: mandá entre 1 y 50 archivos"})
    elif len(archivos) > ARCHIVOS_MAX:
        detalles.append({"campo": "archivos", "problema": f"hasta {ARCHIVOS_MAX} archivos, llegaron {len(archivos)}"})
    elif any(isinstance(archivo, str) for archivo in archivos):
        detalles.append({"campo": "archivos", "problema": "tiene que ser un archivo, llegó texto"})

    operaciones = None
    textos = formulario.getlist("operaciones")
    if len(textos) != 1 or not isinstance(textos[0], str):
        detalles.append({"campo": "operaciones", "problema": "falta, o llegó más de una vez o como archivo"})
    else:
        operaciones, problema = _operaciones(textos[0])
        if problema:
            detalles.append({"campo": "operaciones", "problema": problema})
    if detalles:
        raise ErrorApi("PAYLOAD_INVALIDO", "el pedido no es válido", detalles)
    return archivos, operaciones


def _operaciones(texto):
    try:
        pasos = json.loads(texto)
    except ValueError:
        return None, "no es JSON válido"
    if not isinstance(pasos, list) or not 1 <= len(pasos) <= PASOS_MAX:
        cantidad = len(pasos) if isinstance(pasos, list) else "algo que no es una lista"
        return None, f"tiene que ser una lista de 1 a {PASOS_MAX} pasos, llegó {cantidad}"
    for i, paso in enumerate(pasos):
        if (not isinstance(paso, dict) or set(paso) != {"operacion", "imagen"}
                or not all(isinstance(valor, str) and valor for valor in paso.values())):
            return None, f'el paso {i} tiene que ser {{"operacion": "...", "imagen": "..."}}'
    return pasos, None


def tamanos(archivos):
    """413 si algún archivo pasa de ARCHIVO_MAX. Se mira antes de leerlos."""
    for i, archivo in enumerate(archivos):
        if archivo.size is not None and archivo.size > ARCHIVO_MAX:
            raise ErrorApi("ARCHIVO_DEMASIADO_GRANDE",
                           f"el archivo {i} ({archivo.filename}) supera los {ARCHIVO_MAX} bytes")


def imagenes(contenidos, nombres):
    """Que cada archivo sea un PNG o JPEG sano y de hasta PIXELES_MAX. Junta todos los inválidos en un 422."""
    detalles = []
    for i, datos in enumerate(contenidos):
        if not (datos.startswith(b"\x89PNG\r\n\x1a\n") or datos.startswith(b"\xff\xd8\xff")):
            detalles.append({"campo": f"archivos[{i}]", "problema": "no es PNG ni JPEG"})
            continue
        try:
            with Image.open(io.BytesIO(datos)) as imagen:
                ancho, alto = imagen.size
                if ancho * alto > PIXELES_MAX:
                    raise ErrorApi("ARCHIVO_DEMASIADO_GRANDE",
                                   f"el archivo {i} ({nombres[i]}) tiene {ancho}x{alto}: más de {PIXELES_MAX} píxeles")
                imagen.verify()
        except ErrorApi:
            raise
        except Exception:
            detalles.append({"campo": f"archivos[{i}]", "problema": "está corrupto"})
    if detalles:
        raise ErrorApi("ARCHIVO_INVALIDO", "hay archivos que no son imágenes válidas", detalles)


def pasos(operaciones, permitidas, ofertas):
    """La imagen de cada paso tiene que estar permitida y tener workers que ofrezcan esa operación.

    `ofertas` es {imagen: operaciones} de los workers registrados ahora mismo (detección dinámica).
    """
    for paso in operaciones:
        if paso["imagen"] not in permitidas:
            raise ErrorApi("IMAGEN_NO_PERMITIDA", f"la imagen {paso['imagen']} no está permitida")
    for paso in operaciones:
        imagen, operacion = paso["imagen"], paso["operacion"]
        if imagen not in ofertas:
            raise ErrorApi("SIN_WORKERS", f"no hay workers activos de la imagen {imagen}",
                           cabeceras={"Retry-After": "10"})
        if operacion not in ofertas[imagen]:
            raise ErrorApi("OPERACION_NO_SOPORTADA",
                           f"la imagen {imagen} no ofrece la operación '{operacion}' "
                           f"(ofrece: {', '.join(sorted(ofertas[imagen]))})")
