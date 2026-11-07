"""El escalador con un Terraform falso: la regla, la ventana para bajar, el drenaje y el teardown."""

import threading
import time

import pytest

import escalador


class TerraformFalso:
    def __init__(self):
        self.cantidad = 0
        self.aplicados = []

    def aplicar(self, cantidad, ip_permitida):
        self.aplicados.append(cantidad)
        self.cantidad = cantidad

    def workers(self):
        return [f"10.0.0.{i}:8080" for i in range(self.cantidad)]


def armar(esperando, en_curso=lambda: [], maximo=4, estabilizacion=30, responde=lambda w: True):
    terraform = TerraformFalso()
    return terraform, escalador.Escalador(terraform, esperando, en_curso, maximo, estabilizacion, umbral=4, margen=2,
                                          ip_publica=lambda: "1.2.3.4/32", responde=responde)


def esperar(condicion, segundos=3):
    limite = time.monotonic() + segundos
    while not condicion():
        assert time.monotonic() < limite, "no pasó a tiempo"
        time.sleep(0.02)


@pytest.mark.parametrize("esperando, vms", [(0, 0), (3, 0), (4, 1), (7, 1), (8, 2), (50, 2)])
def test_una_vm_cada_umbral_de_cola_hasta_el_maximo(esperando, vms):
    assert escalador.deseadas(esperando, 2, umbral=4) == vms


@pytest.mark.parametrize("esperando, vms", [(1, 0), (2, 1), (5, 1), (6, 2)])
def test_con_margen_se_mantienen_hasta_dos_por_debajo_del_umbral(esperando, vms):
    assert escalador.deseadas(esperando, 2, umbral=4, margen=2) == vms


def test_la_ventana_baja_recien_despues_de_30_s_y_al_maximo_pedido():
    ventana = escalador.Ventana(30)
    assert ventana.bajar_a(2, 4, ahora=0) is None
    assert ventana.bajar_a(1, 4, ahora=20) is None
    assert ventana.bajar_a(0, 4, ahora=30) == 2       # lo más que pidió en la ventana
    assert ventana.bajar_a(3, 3, ahora=40) is None    # si vuelve a pedir lo mismo, se reinicia
    assert ventana.desde is None


def test_sube_enseguida_y_suma_las_vms_cuando_responden():
    terraform, e = armar(esperando=lambda: 8)
    e.ajustar(ahora=0)
    assert terraform.aplicados == [2]
    esperar(lambda: len(e.nube()) == 2)
    assert e.estado()["arrancando"] == []


def test_una_vm_que_no_responde_no_se_usa():
    terraform, e = armar(esperando=lambda: 4, responde=lambda w: False)
    e.ajustar(ahora=0)
    time.sleep(0.1)
    assert e.nube() == [] and e.estado()["arrancando"] == ["10.0.0.0:8080"]
    e.detener.set()


def test_baja_tras_la_ventana_y_espera_al_pedido_en_curso():
    cola = {"esperando": 16}
    usando = ["sobel-worker:8080", "10.0.0.2:8080", "10.0.0.3:8080"]
    terraform, e = armar(esperando=lambda: cola["esperando"], en_curso=lambda: list(usando))
    e.ajustar(ahora=0)
    esperar(lambda: len(e.nube()) == 4)
    cola["esperando"] = 7                              # alcanzan 2
    e.ajustar(ahora=10)
    assert terraform.aplicados == [4]                  # todavía dentro de la ventana
    hilo = threading.Thread(target=e.ajustar, kwargs={"ahora": 41})
    hilo.start()
    esperar(lambda: e.nube() == ["10.0.0.0:8080", "10.0.0.1:8080"])   # primero salen de la lista
    time.sleep(0.2)
    assert terraform.aplicados == [4]                  # el pedido en curso usa las que sobran: no se borran
    usando.clear()                                     # terminó el pedido
    hilo.join(timeout=3)
    assert terraform.aplicados == [4, 2]


def test_la_cola_quieta_cerca_del_umbral_no_borra_la_vm():
    cola = {"esperando": 4}
    terraform, e = armar(esperando=lambda: cola["esperando"])
    e.ajustar(ahora=0)
    for ahora, esperando in [(10, 3), (20, 2), (40, 3), (60, 2)]:   # baja y sube de a uno: la VM la sostiene
        cola["esperando"] = esperando
        e.ajustar(ahora=ahora)
    assert terraform.aplicados == [1]
    cola["esperando"] = 1                                           # ahora sí sobra
    e.ajustar(ahora=70)
    e.ajustar(ahora=100)
    assert terraform.aplicados == [1, 0]


def test_con_maximo_cero_nunca_toca_terraform():
    terraform, e = armar(esperando=lambda: 20, maximo=0)
    e.ajustar(ahora=0)
    e.apagar()
    assert terraform.aplicados == []


def test_al_apagar_deja_cero_vms():
    terraform, e = armar(esperando=lambda: 8)
    e.ajustar(ahora=0)
    e.apagar()
    assert terraform.aplicados == [2, 0] and e.nube() == []
