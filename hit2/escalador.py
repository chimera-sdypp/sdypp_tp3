"""El escalador del Hit #2: ajusta las VMs de GCP a los pedidos que esperan en la cola.

Cada `paso` segundos mira el largo de la cola, que es el trabajo pendiente de verdad (no es lo mismo esperar 3 fotos de
1 MB que 3 de 100 MB): una VM más cada SOBEL_UMBRAL pedidos en espera (3: con 3 una VM, con 6 dos), hasta
SOBEL_MAX_VMS (2). Sube enseguida. Para bajar, cada VM se mantiene mientras la cola no baje más de SOBEL_MARGEN pedidos
(1) de su umbral (la primera hasta que quede 1 esperando, la segunda 4) durante SOBEL_ESTABILIZACION segundos.

El umbral sale de dos cuentas: una VM tarda ~65 s en atender desde que se pide, y en ese tiempo la cola crece ~1
pedido más (con un ritmo justo para que la VM nueva la sostenga); además sube y baja de a uno con cada pedido que entra
y sale. Así, con la primera VM la cola se queda quieta entre el umbral y el umbral + 2, y el segundo umbral tiene que
estar más arriba: 3 es el más chico que cumple. Un umbral más grande también anda, pero la cola tarda más en llegar. Antes de borrar una VM la saca de la lista de workers y espera al
pedido en curso, que podría estar usándola.

Las VMs las crea y las borra Terraform (hit2/infra) con las credenciales de Workload Identity del pod: no hay claves.
"""

import json
import os
import subprocess
import threading
import time
import urllib.request

import registro

log, _ = registro.configurar("escalador")


def deseadas(esperando, maximo, umbral, margen=0):
    """VMs para la cola: una cada `umbral` pedidos en espera, hasta `maximo`. Con `margen`, las que se mantienen."""
    return min(maximo, (esperando + margen) // umbral)


class Ventana:
    """La ventana de estabilización para bajar: desde cuándo alcanzan menos VMs y el máximo pedido en ese tiempo."""

    def __init__(self, segundos):
        self.segundos = segundos
        self.desde = None
        self.tope = 0

    def bajar_a(self, deseadas, actuales, ahora):
        """A cuántas VMs bajar ahora, o None si todavía no corresponde."""
        if deseadas >= actuales:
            self.desde = None
            return None
        if self.desde is None:
            self.desde, self.tope = ahora, deseadas
        self.tope = max(self.tope, deseadas)
        if ahora - self.desde < self.segundos:
            return None
        self.desde = None
        return self.tope


class Terraform:
    """`terraform apply` y `output` sobre hit2/infra. El proyecto y la imagen llegan por TF_VAR_proyecto/imagen."""

    def __init__(self, directorio, bucket):
        self.directorio = directorio
        self.bucket = bucket
        self.iniciado = False

    def _correr(self, *argumentos):
        resultado = subprocess.run(["terraform", f"-chdir={self.directorio}", *argumentos, "-no-color"],
                                   capture_output=True, text=True)
        if resultado.returncode != 0:
            raise RuntimeError(f"terraform {argumentos[0]} falló: {resultado.stderr.strip()[-500:]}")
        return resultado.stdout

    def _iniciar(self):
        if not self.iniciado:
            self._correr("init", "-input=false", "-reconfigure", f"-backend-config=bucket={self.bucket}")
            self.iniciado = True

    def aplicar(self, cantidad, ip_permitida):
        self._iniciar()
        self._correr("apply", "-auto-approve", "-input=false", "-lock-timeout=120s", f"-var=cantidad={cantidad}",
                     f"-var=ip_permitida={ip_permitida}")

    def workers(self):
        """Las VMs que hay según el estado, en orden de índice (terraform borra las últimas)."""
        self._iniciar()
        salida = json.loads(self._correr("output", "-json")).get("workers", {}).get("value")
        return [w for w in (salida or "").split(",") if w]


def ip_publica():
    """La IP con la que salen los pods: el firewall de las VMs abre el 8080 sólo a ella."""
    with urllib.request.urlopen("https://ifconfig.me/ip", timeout=10) as respuesta:
        return respuesta.read().decode().strip() + "/32"


def responde(worker):
    try:
        with urllib.request.urlopen(f"http://{worker}/health", timeout=2):
            return True
    except OSError:
        return False


class Escalador:
    """`esperando()`: los pedidos en cola; `en_curso()`: los workers que usa el pedido que se está atendiendo."""

    def __init__(self, terraform, esperando, en_curso, maximo, estabilizacion, umbral=3, margen=1, paso=5,
                 ip_publica=ip_publica, responde=responde):
        self.terraform = terraform
        self.esperando = esperando
        self.en_curso = en_curso
        self.maximo = maximo
        self.umbral = umbral
        self.margen = margen
        self.ventana = Ventana(estabilizacion)
        self.paso = paso
        self.ip_publica = ip_publica
        self.responde = responde
        self.pedidas = 0
        self.listas = []          # las VMs que responden: el master las suma a los pedidos siguientes
        self.arrancando = set()
        self._lock = threading.Lock()
        self.detener = threading.Event()

    def nube(self):
        with self._lock:
            return list(self.listas)

    def estado(self):
        with self._lock:
            return {"pedidas": self.pedidas, "listas": list(self.listas), "arrancando": sorted(self.arrancando),
                    "maximo": self.maximo, "umbral": self.umbral, "margen": self.margen}

    def correr(self):
        if self.maximo > 0:
            self._retomar()
        while not self.detener.wait(self.paso):
            try:
                self.ajustar(time.monotonic())
            except Exception as error:   # un apply que falla no tira abajo al master: se reintenta en 30 s
                log.error("no se pudo ajustar las VMs: %s", error)
                self.detener.wait(30)

    def _retomar(self):
        """Al arrancar (p. ej. si el pod se reinició con VMs prendidas) toma las que ya hay en el estado."""
        try:
            existentes = self.terraform.workers()
        except RuntimeError as error:
            log.error("no se pudo leer el estado de Terraform: %s", error)
            return
        if existentes:
            log.info("hay %d VMs de antes: se suman cuando respondan", len(existentes))
        self.pedidas = len(existentes)
        self._vigilar(existentes)

    def ajustar(self, ahora):
        if self.maximo == 0:
            return
        esperando = self.esperando()
        pedir = deseadas(esperando, self.maximo, self.umbral)
        if pedir > self.pedidas:
            log.info("%d pedidos esperando → %d VMs (había %d): terraform apply", esperando, pedir, self.pedidas)
            self.ventana.desde = None
            self._aplicar(pedir)
            return
        mantener = deseadas(esperando, self.maximo, self.umbral, self.margen)
        bajar = self.ventana.bajar_a(mantener, self.pedidas, ahora)
        if bajar is not None:
            log.info("%d s con pocos pedidos esperando (%d) → %d VMs (había %d)", self.ventana.segundos, esperando,
                     bajar, self.pedidas)
            self._bajar(bajar)

    def _aplicar(self, cantidad):
        self.terraform.aplicar(cantidad, self.ip_publica())
        self.pedidas = cantidad
        actuales = self.terraform.workers() if cantidad else []
        with self._lock:   # las que ya no existen dejan de usarse
            self.listas = [w for w in self.listas if w in actuales]
            self.arrancando &= set(actuales)
        self._vigilar(actuales)

    def _bajar(self, cantidad):
        """Saca de la lista las VMs que sobran, espera al pedido en curso y recién ahí las borra."""
        sobran = set(self.terraform.workers()[cantidad:])
        with self._lock:
            self.listas = [w for w in self.listas if w not in sobran]
            self.arrancando -= sobran
        while sobran & set(self.en_curso()) and not self.detener.is_set():
            time.sleep(1)
        self._aplicar(cantidad)

    def _vigilar(self, workers):
        """Cada VM nueva entra en la lista cuando responde /health (el worker ya arrancó en la VM)."""
        for worker in workers:
            with self._lock:
                if worker in self.listas or worker in self.arrancando:
                    continue
                self.arrancando.add(worker)
            threading.Thread(target=self._esperar_vm, args=(worker,), daemon=True).start()

    def _esperar_vm(self, worker):
        while not self.detener.is_set():
            with self._lock:
                if worker not in self.arrancando:   # la borraron antes de que respondiera
                    return
            if self.responde(worker):
                with self._lock:
                    if worker in self.arrancando:
                        self.arrancando.discard(worker)
                        self.listas.append(worker)
                        log.info("VM %s lista: %d en la nube", worker, len(self.listas))
                return
            time.sleep(3)

    def apagar(self):
        """Teardown: deja 0 VMs. Lo llama el master al recibir SIGTERM."""
        self.detener.set()
        if self.maximo > 0 and self.pedidas > 0:
            log.info("teardown: borrando las VMs")
            with self._lock:
                self.listas, self.arrancando = [], set()
            self.terraform.aplicar(0, self.ip_publica())
            self.pedidas = 0


def desde_entorno(esperando, en_curso):
    """El escalador con la configuración de las variables de entorno del pod."""
    terraform = Terraform(os.environ.get("SOBEL_INFRA", "infra"), os.environ.get("TF_BUCKET", ""))
    return Escalador(terraform, esperando, en_curso, maximo=int(os.environ.get("SOBEL_MAX_VMS", 2)),
                     estabilizacion=float(os.environ.get("SOBEL_ESTABILIZACION", 30)),
                     umbral=int(os.environ.get("SOBEL_UMBRAL", 3)), margen=int(os.environ.get("SOBEL_MARGEN", 1)))
