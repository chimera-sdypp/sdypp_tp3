#!/usr/bin/env python3
"""
generar_datos.py — Generador de datos sintéticos e imágenes de prueba para k6.
Hit #3 — Sistemas Distribuidos y Paralelos (Grupo Cerberus)

Genera:
1. Imágenes calibradas en PNG para los diferentes tamaños V1 (1 KB a 50 MB/100 MB).
2. Casos de prueba inválidos (imagen corrupta, archivo de texto plano).
3. Tokens JWT de prueba con roles 'usuario' y 'admin', y un token expirado,
   utilizando únicamente la biblioteca estándar de Python (hmac, hashlib, base64).
"""

import os
import sys
import json
import base64
import hmac
import hashlib
import time
from pathlib import Path

# Directorio base para datos
DIR_BASE = Path(__file__).resolve().parent
DIR_DATOS = DIR_BASE / "datos"

def obtener_secreto() -> str:
    """Obtiene el secreto JWT desde entorno o desde .env.secrets generado por crear_secretos.sh."""
    if "SOBEL_SECRETO" in os.environ and os.environ["SOBEL_SECRETO"]:
        return os.environ["SOBEL_SECRETO"]
    for p in [DIR_BASE / ".env.secrets", DIR_BASE.parent.parent / ".env.secrets"]:
        if p.exists():
            try:
                for line in p.read_text(encoding="utf-8").splitlines():
                    if line.startswith("SOBEL_SECRETO="):
                        val = line.split("=", 1)[1].strip()
                        if val:
                            return val
            except Exception:
                pass
    return "secreto-sobel-super-seguro"


SOBEL_SECRETO_DEFAULT = obtener_secreto()


def b64url(data: bytes) -> str:
    """Codifica en base64url sin relleno '=', según especificación JWT."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def emitir_jwt(sub: str, role: str, secret: str, ttl_s: int = 3600, exp: int = None) -> str:
    """Genera un token JWT HS256 compatible con el backend de Sobel."""
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": sub,
        "role": role,
        "exp": int(time.time()) + ttl_s if exp is None else exp
    }
    h_str = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p_str = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    msg = f"{h_str}.{p_str}".encode("utf-8")
    sig = b64url(hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).digest())
    return f"{h_str}.{p_str}.{sig}"


def generar_tokens(secret: str = SOBEL_SECRETO_DEFAULT, cant_usuarios: int = 50) -> Path:
    """Genera tokens para la matriz R5: 1 a 50 usuarios, admin y un token expirado."""
    tokens = {
        "usuarios": {},
        "admin": emitir_jwt("admin_k6", "admin", secret, ttl_s=86400),
        "expirado": emitir_jwt("bot_expirado", "usuario", secret, exp=int(time.time()) - 3600),
    }

    for i in range(1, cant_usuarios + 1):
        user_name = f"user_{i:02d}"
        tokens["usuarios"][user_name] = emitir_jwt(user_name, "usuario", secret, ttl_s=86400)

    archivo_tokens = DIR_DATOS / "tokens.json"
    with open(archivo_tokens, "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)

    print(f"[OK] Generados {cant_usuarios} tokens de usuario, 1 admin y 1 expirado en: {archivo_tokens}")
    return archivo_tokens


def generar_imagenes():
    """Genera imágenes PNG de tamaños calibrados para las pruebas de carga."""
    try:
        from PIL import Image
        import numpy as np
    except ImportError:
        print("[ERROR] Se requiere PIL (Pillow) y numpy para generar imágenes.")
        print("Instalar con: pip install pillow numpy")
        sys.exit(1)

    DIR_DATOS.mkdir(parents=True, exist_ok=True)

    # 1. Imagen ~1 KB (32x32)
    path_1k = DIR_DATOS / "img_1kb.png"
    arr_1k = np.random.randint(0, 256, (32, 32, 3), dtype=np.uint8)
    Image.fromarray(arr_1k).save(path_1k, format="PNG")
    print(f"[OK] Creada {path_1k.name} ({path_1k.stat().st_size} bytes)")

    # 2. Imagen ~1 MB (600x600 con ruido RGB)
    path_1m = DIR_DATOS / "img_1mb.png"
    arr_1m = np.random.randint(0, 256, (600, 600, 3), dtype=np.uint8)
    Image.fromarray(arr_1m).save(path_1m, format="PNG")
    print(f"[OK] Creada {path_1m.name} ({path_1m.stat().st_size / 1024 / 1024:.2f} MB)")

    # 3. Imagen ~10 MB (1850x1850 con ruido RGB)
    path_10m = DIR_DATOS / "img_10mb.png"
    arr_10m = np.random.randint(0, 256, (1850, 1850, 3), dtype=np.uint8)
    Image.fromarray(arr_10m).save(path_10m, format="PNG")
    print(f"[OK] Creada {path_10m.name} ({path_10m.stat().st_size / 1024 / 1024:.2f} MB)")

    # 4. Imagen ~50 MB (4150x4150 con ruido RGB, compresión nivel 1 para velocidad)
    path_50m = DIR_DATOS / "img_50mb.png"
    arr_50m = np.random.randint(0, 256, (4150, 4150, 3), dtype=np.uint8)
    Image.fromarray(arr_50m).save(path_50m, format="PNG", compress_level=1)
    print(f"[OK] Creada {path_50m.name} ({path_50m.stat().st_size / 1024 / 1024:.2f} MB)")

    # 5. Imagen corrupta (bytes aleatorios con extensión .png pero sin cabecera PNG)
    path_corrupta = DIR_DATOS / "corrupta.png"
    with open(path_corrupta, "wb") as f:
        f.write(b"NOT_A_PNG_FILE_HEADER_" + os.urandom(1024))
    print(f"[OK] Creada {path_corrupta.name} (archivo inválido/corrupto)")

    # 6. Archivo de texto plano (para caso tipo de contenido inválido)
    path_txt = DIR_DATOS / "no_imagen.txt"
    with open(path_txt, "w", encoding="utf-8") as f:
        f.write("Este no es un archivo de imagen válido.")
    print(f"[OK] Creada {path_txt.name} (archivo de texto)")


def main():
    DIR_DATOS.mkdir(parents=True, exist_ok=True)
    secreto = os.environ.get("SOBEL_SECRETO", SOBEL_SECRETO_DEFAULT)
    print(f"=== Generando datos sintéticos para pruebas de carga k6 ===")
    print(f"Directorio de salida: {DIR_DATOS}")
    print(f"Secreto JWT en uso: {secreto[:4]}***")
    
    generar_tokens(secret=secreto, cant_usuarios=50)
    generar_imagenes()
    print("=== Datos de prueba listos exitosamente ===")


if __name__ == "__main__":
    main()
