"""Claves de Redis del contrato (§10). Todos los servicios las arman acá: una clave mal escrita no falla,
sólo hace que un servicio no encuentre lo que escribió otro."""

TERMINADOS = "terminados"


def job(job_id):
    return f"job:{job_id}"


def entrada(job_id, archivo):
    return f"job:{job_id}:entrada:{archivo}"


def geometria(job_id, archivo):
    return f"job:{job_id}:geometria:{archivo}"


def fragmento(job_id, archivo, indice, paso):
    """El fragmento `indice` del archivo después de aplicar `paso` operaciones: el split escribe el paso 0."""
    return f"job:{job_id}:frag:{archivo}:{indice}:{paso}"


def workers(job_id):
    return f"job:{job_id}:workers"


def worker(nombre):
    """El anuncio de un worker activo; `nombre` es su hostname."""
    return f"worker:{nombre}"
