"""Sobre común de respuesta y errores tipados (contrato §6).

Toda respuesta es {"codigo", "contenido"}; en un error, contenido = {"error": {tipo, mensaje, detalles}}.
"""

from fastapi.responses import JSONResponse

CODIGOS = {
    "CUERPO_INVALIDO": 400,
    "NO_AUTORIZADO": 401,
    "ROL_INSUFICIENTE": 403,
    "IMAGEN_NO_PERMITIDA": 403,
    "JOB_INEXISTENTE": 404,
    "RUTA_INEXISTENTE": 404,
    "METODO_NO_PERMITIDO": 405,
    "JOB_NO_TERMINADO": 409,
    "ARCHIVO_DEMASIADO_GRANDE": 413,
    "TIPO_DE_CONTENIDO": 415,
    "PAYLOAD_INVALIDO": 422,
    "ARCHIVO_INVALIDO": 422,
    "OPERACION_NO_SOPORTADA": 422,
    "DEMASIADOS_PEDIDOS": 429,
    "ERROR_INTERNO": 500,
    "SERVICIO_NO_DISPONIBLE": 503,
    "SIN_WORKERS": 503,
}


class ErrorApi(Exception):
    def __init__(self, tipo, mensaje, detalles=None, cabeceras=None):
        super().__init__(mensaje)
        self.tipo = tipo
        self.codigo = CODIGOS[tipo]
        self.mensaje = mensaje
        self.detalles = detalles or []
        self.cabeceras = cabeceras or {}


def sobre(codigo, contenido, cabeceras=None):
    return JSONResponse({"codigo": codigo, "contenido": contenido}, status_code=codigo, headers=cabeceras)


def respuesta_de_error(error):
    contenido = {"error": {"tipo": error.tipo, "mensaje": error.mensaje, "detalles": error.detalles}}
    return sobre(error.codigo, contenido, error.cabeceras)
