import json

from comun import colas

IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"


class CanalQueRegistra:
    """Registra lo que se declara y publica, con la misma firma que un canal de pika."""

    def __init__(self):
        self.colas, self.exchanges, self.ataduras, self.publicados = {}, {}, set(), []

    def queue_declare(self, queue, durable=False, arguments=None):
        self.colas[queue] = {"durable": durable, "arguments": arguments or {}}

    def exchange_declare(self, exchange, exchange_type, durable=False):
        self.exchanges[exchange] = {"tipo": exchange_type, "durable": durable}

    def queue_bind(self, queue, exchange, routing_key=None):
        self.ataduras.add((queue, exchange, routing_key))

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.publicados.append((exchange, routing_key, body, properties))


def test_declarar_crea_las_colas_fijas_quorum_y_durables():
    canal = CanalQueRegistra()

    colas.declarar(canal)

    assert set(canal.colas) == {"sobel.split", "sobel.dlq", "sobel.dlq.agotados", "sobel.joiner"}
    for nombre, cola in canal.colas.items():
        assert cola["durable"], nombre
        assert cola["arguments"]["x-queue-type"] == "quorum", nombre


def test_declarar_ata_la_dlq_al_dlx_y_el_joiner_al_fanout_de_resultados():
    canal = CanalQueRegistra()

    colas.declarar(canal)

    assert canal.exchanges == {"sobel.dlx": {"tipo": "direct", "durable": True},
                               "sobel.resultados": {"tipo": "fanout", "durable": True}}
    assert canal.ataduras == {("sobel.dlq", "sobel.dlx", "sobel.dlq"),
                              ("sobel.joiner", "sobel.resultados", "")}



def test_declarar_tareas_crea_la_cola_de_la_imagen_con_dead_letter_a_la_dlq(monkeypatch):
    monkeypatch.delenv("SOBEL_LIMITE_ENTREGAS", raising=False)
    canal = CanalQueRegistra()

    cola = colas.declarar_tareas(canal, IMAGEN)

    assert cola == "sobel.tareas.ghcr.io/chimera-sdypp/sobel-worker:1.0.0"
    assert canal.colas == {cola: {"durable": True, "arguments": {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "sobel.dlx",
        "x-dead-letter-routing-key": "sobel.dlq",
        "x-delivery-limit": 3,
    }}}


def test_el_limite_de_entregas_sale_de_sobel_limite_entregas(monkeypatch):
    monkeypatch.setenv("SOBEL_LIMITE_ENTREGAS", "5")
    canal = CanalQueRegistra()

    cola = colas.declarar_tareas(canal, IMAGEN)

    assert canal.colas[cola]["arguments"]["x-delivery-limit"] == 5


def test_publicar_manda_json_persistente_por_el_exchange_por_defecto():
    canal = CanalQueRegistra()

    colas.publicar(canal, "sobel.split", {"job_id": "3f6c2a9e", "archivo": 0})

    [(exchange, routing_key, cuerpo, propiedades)] = canal.publicados
    assert (exchange, routing_key) == ("", "sobel.split")
    assert json.loads(cuerpo) == {"job_id": "3f6c2a9e", "archivo": 0}
    assert propiedades.content_type == "application/json"
    assert propiedades.delivery_mode == 2


def test_publicar_al_fanout_de_resultados_con_las_mismas_propiedades():
    canal = CanalQueRegistra()

    colas.publicar(canal, "", {"job_id": "3f6c2a9e", "fragmento": 3}, exchange="sobel.resultados")

    [(exchange, routing_key, cuerpo, propiedades)] = canal.publicados
    assert (exchange, routing_key) == ("sobel.resultados", "")
    assert json.loads(cuerpo) == {"job_id": "3f6c2a9e", "fragmento": 3}
    assert (propiedades.content_type, propiedades.delivery_mode) == ("application/json", 2)


def test_declarar_exclusiva_ata_una_cola_con_nombre_del_broker_al_fanout():
    class Canal(CanalQueRegistra):
        def queue_declare(self, queue, exclusive=False, auto_delete=False):
            self.pedido = (queue, exclusive, auto_delete)
            return type("Respuesta", (), {"method": type("Metodo", (), {"queue": "amq.gen-abc"})})

    canal = Canal()

    assert colas.declarar_exclusiva(canal) == "amq.gen-abc"
    assert canal.pedido == ("", True, True)
    assert canal.ataduras == {("amq.gen-abc", "sobel.resultados", "")}
