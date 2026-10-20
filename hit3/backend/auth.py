"""Identidad por request (R4): JWT HS256 con `sub`, `role` y `exp`, sin sesión en el servidor."""

import time
from typing import NamedTuple

import jwt

from backend.errores import ErrorApi

ROLES = ("usuario", "admin")
LARGO_MINIMO_SECRETO = 32


class Identidad(NamedTuple):
    usuario: str
    rol: str

    @property
    def es_admin(self):
        return self.rol == "admin"


def autenticar(cabecera, secreto):
    """Valida `Authorization: Bearer <token>`. 401 si falta o no vale; 403 si el rol no existe."""
    partes = (cabecera or "").split(None, 1)
    if len(partes) != 2 or partes[0].lower() != "bearer":
        raise _no_autorizado("falta el token: Authorization: Bearer <token>")
    try:
        datos = jwt.decode(partes[1].strip(), secreto, algorithms=["HS256"], options={"require": ["exp", "sub"]})
    except jwt.ExpiredSignatureError:
        raise _no_autorizado("el token venció") from None
    except jwt.InvalidTokenError:
        raise _no_autorizado("token inválido") from None
    usuario = datos["sub"]
    if not isinstance(usuario, str) or not usuario.strip():
        raise _no_autorizado("token inválido: 'sub' tiene que ser un string no vacío")
    if datos.get("role") not in ROLES:
        raise ErrorApi("ROL_INSUFICIENTE", "el token no trae un rol válido (usuario o admin)")
    return Identidad(usuario.strip(), datos["role"])


def exigir_admin(identidad, ruta):
    if not identidad.es_admin:
        raise ErrorApi("ROL_INSUFICIENTE", f"{ruta} es sólo para admin (tu rol: {identidad.rol})")


def limitar(redis, usuario, por_minuto, reloj=time.time):
    """Cuenta los pedidos del usuario en el minuto actual. El contador está en Redis: vale para todas las réplicas."""
    ahora = reloj()
    clave = f"limite:{usuario}:{int(ahora // 60)}"
    tuberia = redis.pipeline()
    tuberia.incr(clave)
    tuberia.expire(clave, 60)
    pedidos, _ = tuberia.execute()
    if pedidos > por_minuto:
        espera = 60 - int(ahora) % 60
        raise ErrorApi("DEMASIADOS_PEDIDOS", f"más de {por_minuto} pedidos por minuto: reintentá en {espera} s",
                       cabeceras={"Retry-After": str(espera)})


def emitir(usuario, rol, secreto, horas=24, reloj=time.time):
    ahora = int(reloj())
    return jwt.encode({"sub": usuario, "role": rol, "iat": ahora, "exp": ahora + int(horas * 3600)},
                      secreto, algorithm="HS256")


def _no_autorizado(mensaje):
    return ErrorApi("NO_AUTORIZADO", mensaje, cabeceras={"WWW-Authenticate": "Bearer"})
