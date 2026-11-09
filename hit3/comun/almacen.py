"""Los píxeles van a un bucket; Redis guarda sólo el puntero (contrato §10).

Cada clave de píxeles de Redis (`job:<id>:entrada:<a>`, `job:<id>:frag:…`, `job:<id>:resultado:<a>`) tiene como
valor el nombre del objeto en el bucket (`job/<id>/entrada/<a>.png`, …). Así Redis queda con el estado de los jobs
y unos bytes por imagen, y lo que pesa vive en el almacenamiento de objetos, que no tiene límite de memoria.

El orden hace seguras las caídas: al escribir, primero el objeto y después el puntero (un puntero nunca apunta a
nada); al borrar, primero el puntero y después el objeto (si se corta en el medio queda un objeto huérfano, que
borra la regla de vencimiento del bucket).

Backend: la API JSON de Google Cloud Storage. En GKE se autentica con Workload Identity (la cuenta de servicio de
Kubernetes del pod), sin claves. Con `SOBEL_ALMACEN_URL` habla con un emulador (fake-gcs-server) sin autenticar:
para el k3s local y los tests de integración.
"""

import os
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import requests

from comun import dinamico

GCS = "https://storage.googleapis.com"
ALCANCE = "https://www.googleapis.com/auth/devstorage.read_write"
HILOS = 8  # subidas y bajadas en paralelo de un mismo pedido, si no hay Redis para leer la perilla `hilos_almacen`
CABECERA = 64 * 1024  # bytes del principio de una imagen que alcanzan para leer su formato y sus dimensiones
SALUD_CADA = 10  # s que se recuerda el resultado de `vivo`: las probes preguntan seguido


class NoDisponible(OSError):
    """El bucket no responde (red, 5xx o credenciales): lo que se estaba haciendo se puede reintentar.

    Es un OSError para que `conexion.con_backoff` lo reintente como a una caída de Redis o de RabbitMQ.
    """


def nombre_de(clave):
    """`job:j1:frag:0:2:1` → `job/j1/frag/0/2/1.png`: el mismo nombre siempre, así reescribir es idempotente."""
    return clave.replace(":", "/") + ".png"


class Almacen:
    """Un bucket de GCS (o del emulador) con lo justo: guardar, leer, borrar y abrir subidas directas."""

    def __init__(self, bucket, url=None, sesion=None, timeout=60, url_publica=None):
        self.bucket = bucket
        self.url = (url or GCS).rstrip("/")
        self.url_publica = (url_publica or self.url).rstrip("/")  # la del emulador, como la ve el cliente
        self.emulador = url is not None
        self.timeout = timeout
        self._sesion = sesion
        self._salud = (0.0, False)
        self._candado = threading.Lock()

    @property
    def sesion(self):
        # Perezosa: crear las credenciales consulta el servidor de metadatos, y en un test no hace falta.
        with self._candado:
            if self._sesion is None:
                if self.emulador:
                    self._sesion = requests.Session()
                else:
                    import google.auth
                    from google.auth.transport.requests import AuthorizedSession
                    credenciales, _ = google.auth.default(scopes=[ALCANCE])
                    self._sesion = AuthorizedSession(credenciales)
            return self._sesion

    def _pedir(self, metodo, url, aceptados, **opciones):
        try:
            respuesta = self.sesion.request(metodo, url, timeout=self.timeout, **opciones)
        except Exception as error:  # requests y google.auth (sin credenciales, metadatos caídos)
            raise NoDisponible(f"{metodo} {url}: {error!r}") from error
        if respuesta.status_code not in aceptados:
            raise NoDisponible(f"{metodo} {url}: {respuesta.status_code} {respuesta.text[:200]}")
        return respuesta

    def _objeto(self, nombre):
        return f"{self.url}/storage/v1/b/{self.bucket}/o/{urllib.parse.quote(nombre, safe='')}"

    def guardar(self, nombre, datos):
        self._pedir("POST", f"{self.url}/upload/storage/v1/b/{self.bucket}/o", (200,),
                    params={"uploadType": "media", "name": nombre}, data=datos,
                    headers={"Content-Type": "image/png"})

    def leer(self, nombre):
        """Los bytes del objeto, o None si no existe."""
        respuesta = self._pedir("GET", self._objeto(nombre), (200, 404), params={"alt": "media"})
        return respuesta.content if respuesta.status_code == 200 else None

    def iniciar_subida(self, nombre, tamano):
        """Abre una sesión de subida resumable para `nombre` y devuelve su URL (contrato §2b).

        Quien tiene la URL sube con un PUT, sin credenciales: la URL misma es la autorización, como una URL firmada.
        Sirve sólo para ese objeto, declara `tamano` bytes (X-Upload-Content-Length; el backend lo revisa al
        confirmar) y GCS la invalida a la semana. La abre el backend con su identidad (Workload Identity): una URL
        firmada V4 necesitaría una cuenta de servicio de Google con clave o el permiso signBlob, y los pods no tienen
        ninguna de las dos.
        """
        respuesta = self._pedir("POST", f"{self.url}/upload/storage/v1/b/{self.bucket}/o", (200, 201),
                                params={"uploadType": "resumable", "name": nombre},
                                headers={"X-Upload-Content-Length": str(tamano)}, json={})
        ubicacion = respuesta.headers.get("Location")
        if not ubicacion:
            raise NoDisponible(f"el bucket no devolvió la URL de la sesión de subida de {nombre}")
        if self.emulador:  # fake-gcs arma la URL con su dirección interna: el cliente lo ve en url_publica
            publica, sesion = urllib.parse.urlsplit(self.url_publica), urllib.parse.urlsplit(ubicacion)
            ubicacion = urllib.parse.urlunsplit((publica.scheme, publica.netloc, sesion.path, sesion.query, ""))
        return ubicacion

    def inicio(self, nombre, cuantos=CABECERA):
        """`(tamaño total, primeros bytes)` del objeto, o None si no existe. Pide un rango: no baja el resto."""
        respuesta = self._pedir("GET", self._objeto(nombre), (200, 206, 404, 416), params={"alt": "media"},
                                headers={"Range": f"bytes=0-{cuantos - 1}"})
        if respuesta.status_code == 404:
            return None
        if respuesta.status_code == 416:  # el objeto está vacío
            return 0, b""
        total = respuesta.headers.get("Content-Range", "").rpartition("/")[2]
        if not total.isdigit():  # 200: el objeto entero (más chico que el rango, o un servidor que no los entiende)
            total = len(respuesta.content)
        return int(total), respuesta.content[:cuantos]

    def borrar(self, nombres):
        """Borra los objetos; uno que ya no está no es un error (una reentrega puede borrar dos veces)."""
        nombres = list(nombres)
        if len(nombres) == 1:
            self._pedir("DELETE", self._objeto(nombres[0]), (200, 204, 404))
            return
        with ThreadPoolExecutor(max_workers=HILOS) as hilos:
            list(hilos.map(lambda n: self._pedir("DELETE", self._objeto(n), (200, 204, 404)), nombres))

    def vivo(self):
        """True si el bucket responde. Recuerda el resultado SALUD_CADA segundos."""
        cuando, resultado = self._salud
        if time.monotonic() - cuando < SALUD_CADA:
            return resultado
        try:
            self._pedir("GET", f"{self.url}/storage/v1/b/{self.bucket}", (200,), params={"fields": "name"})
            resultado = True
        except NoDisponible:
            resultado = False
        self._salud = (time.monotonic(), resultado)
        return resultado

    def asegurar_bucket(self):
        """Sólo con el emulador: crea el bucket si no existe. En GCS lo crea la infraestructura (con su regla de
        vencimiento y el permiso para la cuenta de servicio), no la aplicación."""
        if self.emulador:
            self._pedir("POST", f"{self.url}/storage/v1/b", (200, 409), params={"project": "local"},
                        json={"name": self.bucket})


class AlmacenMemoria:
    """El mismo contrato que `Almacen`, en un dict: para los tests. `caido = True` simula que el bucket no responde."""

    def __init__(self):
        self.objetos = {}
        self.subidas = {}  # nombre -> tamaño de las sesiones de subida abiertas
        self.caido = False

    def _revisar(self):
        if self.caido:
            raise NoDisponible("el bucket de prueba está caído")

    def guardar(self, nombre, datos):
        self._revisar()
        self.objetos[nombre] = bytes(datos)

    def leer(self, nombre):
        self._revisar()
        return self.objetos.get(nombre)

    def iniciar_subida(self, nombre, tamano):
        self._revisar()
        self.subidas[nombre] = tamano
        return f"https://storage.googleapis.com/upload/storage/v1/b/memoria/o?uploadType=resumable&upload_id={nombre}"

    def inicio(self, nombre, cuantos=CABECERA):
        self._revisar()
        datos = self.objetos.get(nombre)
        return None if datos is None else (len(datos), datos[:cuantos])

    def borrar(self, nombres):
        self._revisar()
        for nombre in nombres:
            self.objetos.pop(nombre, None)

    def vivo(self):
        return not self.caido

    def asegurar_bucket(self):
        self._revisar()


def desde_entorno():
    """SOBEL_ALMACEN_BUCKET (obligatoria); SOBEL_ALMACEN_URL y SOBEL_ALMACEN_URL_PUBLICA, sólo para el emulador (la
    segunda es cómo lo alcanza el cliente que sube directo: en GKE las URLs de subida ya son de storage.googleapis.com)."""
    bucket = os.environ.get("SOBEL_ALMACEN_BUCKET")
    if not bucket:
        raise RuntimeError("falta la variable de entorno SOBEL_ALMACEN_BUCKET")
    return Almacen(bucket, url=os.environ.get("SOBEL_ALMACEN_URL") or None,
                   url_publica=os.environ.get("SOBEL_ALMACEN_URL_PUBLICA") or None)


# --- el puntero en Redis -----------------------------------------------------------------------------------------

def escribir(redis, almacen, clave, datos, ttl):
    """Guarda `datos` en el bucket y deja en `clave` (Redis) el puntero, que vence en `ttl` s."""
    nombre = nombre_de(clave)
    almacen.guardar(nombre, datos)
    redis.set(clave, nombre, ex=ttl)


def escribir_varios(redis, almacen, pares, ttl):
    """Como `escribir` para varios (clave, datos): sube en paralelo y deja todos los punteros juntos."""
    pares = list(pares)
    with ThreadPoolExecutor(max_workers=dinamico.leer(redis)["hilos_almacen"]) as hilos:
        list(hilos.map(lambda par: almacen.guardar(nombre_de(par[0]), par[1]), pares))
    with redis.pipeline() as tuberia:
        for clave, _ in pares:
            tuberia.set(clave, nombre_de(clave), ex=ttl)
        tuberia.execute()


def leer(redis, almacen, clave):
    """Los bytes a los que apunta `clave`, o None si no hay puntero (o el objeto ya no está)."""
    puntero = redis.get(clave)
    return None if puntero is None else almacen.leer(puntero.decode())


def leer_varios(redis, almacen, claves):
    """Como `leer` para varias claves, en el mismo orden; baja en paralelo."""
    punteros = redis.mget(claves) if claves else []
    with ThreadPoolExecutor(max_workers=dinamico.leer(redis)["hilos_almacen"]) as hilos:
        return list(hilos.map(lambda p: None if p is None else almacen.leer(p.decode()), punteros))


def borrar(redis, almacen, *claves):
    """Borra los punteros y después los objetos."""
    if not claves:
        return
    punteros = [p.decode() for p in redis.mget(claves) if p is not None]
    redis.delete(*claves)
    if punteros:
        almacen.borrar(punteros)
