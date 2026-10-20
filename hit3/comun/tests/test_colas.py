import json

from comun import colas


class CanalFalso:
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
    canal = CanalFalso()

    colas.declarar(canal)

    assert set(canal.colas) == {"sobel.split", "sobel.dlq", "sobel.dlq.agotados", "sobel.joiner"}
    for nombre, cola in canal.colas.items():
        assert cola["durable"], nombre
        assert cola["arguments"]["x-queue-type"] == "quorum", nombre


def test_declarar_ata_la_dlq_al_dlx_y_el_joiner_al_fanout_de_resultados():
    canal = CanalFalso()

    colas.declarar(canal)

    assert canal.exchanges == {"sobel.dlx": {"tipo": "direct", "durable": True},
                               "sobel.resultados": {"tipo": "fanout", "durable": True}}
    assert canal.ataduras == {("sobel.dlq", "sobel.dlx", "sobel.dlq"),
                              ("sobel.joiner", "sobel.resultados", "")}


IMAGEN = "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"


def test_declarar_tareas_crea_la_cola_de_la_imagen_con_dead_letter_a_la_dlq(monkeypatch):
    monkeypatch.delenv("SOBEL_LIMITE_ENTREGAS", raising=False)
    canal = CanalFalso()

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
    canal = CanalFalso()

    cola = colas.declarar_tareas(canal, IMAGEN)

    assert canal.colas[cola]["arguments"]["x-delivery-limit"] == 5


def test_publicar_manda_json_persistente_por_el_exchange_por_defecto():
    canal = CanalFalso()

    colas.publicar(canal, "sobel.split", {"job_id": "3f6c2a9e", "archivo": 0})

    [(exchange, routing_key, cuerpo, propiedades)] = canal.publicados
    assert (exchange, routing_key) == ("", "sobel.split")
    assert json.loads(cuerpo) == {"job_id": "3f6c2a9e", "archivo": 0}
    assert propiedades.content_type == "application/json"
    assert propiedades.delivery_mode == 2
