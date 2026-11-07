"""Las imágenes de los pedidos en disco, en el volumen del master: la de entrada mientras espera en la cola y el
resultado durante SOBEL_VENCIMIENTO segundos. Redis guarda sólo el estado de cada pedido: las fotos de la demo pesan
100 MB cada una y en RAM llegaban a ocupar GB.

El disco no vence solo como Redis: `limpiar()` (el master la llama cada 30 s) borra los resultados vencidos, los que
pasan de `max_resultados` (los más viejos primero) y las entradas de pedidos que ya no existen. Y `hay_lugar()` frena
los pedidos nuevos antes de que el volumen pase de `maximo` bytes o al disco le quede menos de 1 GiB.
"""

import os
import shutil
import time

LIBRE_MINIMO = 2**30
VIDA_TEMPORAL = 600   # segundos: un .tmp más viejo es de una escritura que se cortó


class SinLugar(Exception):
    """No entra otra imagen sin pasar el máximo del volumen."""


class Almacen:
    def __init__(self, directorio, vencimiento, max_resultados, maximo):
        self.directorio = directorio
        self.vencimiento = vencimiento
        self.max_resultados = max_resultados
        self.maximo = maximo
        os.makedirs(directorio, exist_ok=True)

    def _ruta(self, id_, tipo):
        return os.path.join(self.directorio, f"{id_}.{tipo}.png")

    def guardar(self, id_, tipo, datos):
        """Escribe a un temporal y lo renombra: nadie lee nunca un archivo a medias."""
        ruta = self._ruta(id_, tipo)
        with open(ruta + ".tmp", "wb") as archivo:
            archivo.write(datos)
        os.replace(ruta + ".tmp", ruta)

    def leer(self, id_, tipo):
        try:
            with open(self._ruta(id_, tipo), "rb") as archivo:
                return archivo.read()
        except FileNotFoundError:
            return None

    def borrar(self, id_, tipo):
        try:
            os.remove(self._ruta(id_, tipo))
        except FileNotFoundError:
            pass

    def _archivos(self):
        """(modificado, tamaño, ruta) de cada archivo del volumen."""
        archivos = []
        for entrada in os.scandir(self.directorio):
            try:
                estado = entrada.stat()
            except FileNotFoundError:   # lo borraron mientras se listaba
                continue
            archivos.append((estado.st_mtime, estado.st_size, entrada.path))
        return archivos

    def usado(self):
        return sum(tamano for _, tamano, _ in self._archivos())

    def hay_lugar(self, tamano):
        libre = shutil.disk_usage(self.directorio).free
        return self.usado() + tamano <= self.maximo and libre - tamano >= LIBRE_MINIMO

    def limpiar(self, ahora=None):
        """Borra lo vencido y los resultados que sobran. Devuelve cuántos archivos borró."""
        ahora = time.time() if ahora is None else ahora
        sobran, resultados = [], []
        for modificado, _, ruta in self._archivos():
            edad = ahora - modificado
            if ruta.endswith(".resultado.png"):
                if edad > self.vencimiento:
                    sobran.append(ruta)
                else:
                    resultados.append((modificado, ruta))
            elif ruta.endswith(".entrada.png"):
                if edad > self.vencimiento * 6:   # lo mismo que vive el pedido en Redis
                    sobran.append(ruta)
            elif edad > VIDA_TEMPORAL:
                sobran.append(ruta)
        resultados.sort()
        sobran += [ruta for _, ruta in resultados[:max(0, len(resultados) - self.max_resultados)]]
        for ruta in sobran:
            try:
                os.remove(ruta)
            except FileNotFoundError:
                pass
        return len(sobran)
