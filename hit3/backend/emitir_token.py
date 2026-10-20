"""Emite un token para la API, firmado con $SOBEL_SECRETO.

python -m backend.emitir_token <usuario> <usuario|admin> [--horas 24]
"""

import argparse
import os
import sys

from backend.auth import ROLES, emitir


def main():
    parser = argparse.ArgumentParser(description="Emite un JWT para la API del Hit #3.")
    parser.add_argument("usuario")
    parser.add_argument("rol", choices=ROLES)
    parser.add_argument("--horas", type=float, default=24)
    args = parser.parse_args()
    secreto = os.environ.get("SOBEL_SECRETO", "")
    if not secreto:
        sys.exit("falta SOBEL_SECRETO")
    print(emitir(args.usuario, args.rol, secreto, args.horas))


if __name__ == "__main__":
    main()
