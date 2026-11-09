"""Configuración dinámica: perillas de rendimiento que se cambian en caliente, sin redesplegar (contrato §4 y §10).

Cada perilla tiene un valor por defecto (el del código, o `SOBEL_<NOMBRE>` del entorno) y se puede pisar en el hash
de Redis `config:dinamica` con `PUT /admin/config`. Cada servicio la lee cuando la usa (una por job en el split, una
por fragmento en el worker y en el joiner): un cambio vale desde el próximo job o fragmento, en todos los pods.
"""

import os

from comun import claves

# nombre: (por defecto, mínimo, máximo, para qué sirve)
ESQUEMA = {
    "mb_por_fragmento": (8, 1, 100, "MB de píxeles (RGB) por fragmento: el split parte cada imagen según su tamaño"),
    "max_partes": (32, 1, 64, "tope de fragmentos por imagen"),
    "umbral_cola": (50, 0, 100_000, "mensajes en la cola de tareas a partir de los cuales el split parte en la mitad "
                                    "de fragmentos (los workers ya están ocupados: más fragmentos sólo suman viajes)"),
    "png_nivel": (1, 0, 9, "compresión de los PNG intermedios (0-9): 1 codifica varias veces más rápido que el 6 "
                           "de Pillow, a cambio de objetos algo más grandes en el bucket"),
    "hilos_almacen": (8, 1, 32, "subidas y bajadas en paralelo contra el bucket por imagen"),
}


def por_defecto(entorno=os.environ):
    """Los valores base: `SOBEL_<NOMBRE>` si está en el entorno y es válido, si no el del código."""
    base = {}
    for nombre, (defecto, minimo, maximo, _) in ESQUEMA.items():
        try:
            valor = int(entorno.get(f"SOBEL_{nombre.upper()}", defecto))
        except ValueError:
            valor = defecto
        base[nombre] = valor if minimo <= valor <= maximo else defecto
    return base


def personalizadas(redis):
    """Sólo las perillas que se pisaron en Redis (y son válidas). Si Redis no responde, ninguna."""
    try:
        crudo = redis.hgetall(claves.CONFIG_DINAMICA) or {}
    except Exception:
        return {}
    resultado = {}
    for clave, valor in crudo.items():
        nombre = clave.decode() if isinstance(clave, bytes) else str(clave)
        if nombre not in ESQUEMA:
            continue
        _, minimo, maximo, _ = ESQUEMA[nombre]
        try:
            numero = int(valor)
        except (TypeError, ValueError):
            continue
        if minimo <= numero <= maximo:
            resultado[nombre] = numero
    return resultado


def leer(redis):
    """La configuración que vale ahora: la base pisada por lo que haya en `config:dinamica`."""
    return {**por_defecto(), **personalizadas(redis)}


def validar(cambios):
    """Revisa el cuerpo de `PUT /admin/config`. Devuelve `(cambios, detalles)`: `detalles` vacío si está bien.

    Un valor `null` vuelve la perilla a su valor por defecto.
    """
    if not isinstance(cambios, dict) or not cambios:
        return None, [{"campo": "config", "problema": "tiene que ser un objeto JSON con al menos una perilla"}]
    detalles = []
    for nombre, valor in cambios.items():
        if nombre not in ESQUEMA:
            detalles.append({"campo": nombre, "problema": f"no existe (son: {', '.join(ESQUEMA)})"})
            continue
        if valor is None:
            continue
        _, minimo, maximo, _ = ESQUEMA[nombre]
        if isinstance(valor, bool) or not isinstance(valor, int) or not minimo <= valor <= maximo:
            detalles.append({"campo": nombre, "problema": f"tiene que ser un entero entre {minimo} y {maximo}, o null"})
    return (None, detalles) if detalles else (cambios, [])


def actualizar(redis, cambios):
    """Aplica cambios ya validados: un número pisa la perilla, `None` la vuelve al valor por defecto."""
    pisar = {nombre: valor for nombre, valor in cambios.items() if valor is not None}
    volver = [nombre for nombre, valor in cambios.items() if valor is None]
    with redis.pipeline() as tuberia:
        if pisar:
            tuberia.hset(claves.CONFIG_DINAMICA, mapping=pisar)
        if volver:
            tuberia.hdel(claves.CONFIG_DINAMICA, *volver)
        tuberia.execute()
    return leer(redis)
