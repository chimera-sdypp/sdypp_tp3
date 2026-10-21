#!/usr/bin/env python3
# ==============================================================================
# generar_datos.py — Generador de Datos Sintéticos e Imágenes Calibradas para k6
# Sistemas Distribuidos y Paralelos — Grupo Cerberus (Salva / Salvador Baez)
# ==============================================================================
# Propósito del script:
# Este módulo se encarga de aprovisionar todos los artefactos de datos necesarios
# para ejecutar la Matriz de Pruebas de Carga R5 y los tests de humo (smoke tests):
#   1. Tokens de Autenticación JWT conformes a RFC 7519 firmados con algoritmo HS256,
#      utilizando exclusivamente la biblioteca estándar de Python (sin dependencias
#      externas de PyJWT o cryptography). Genera identidades para usuarios concurrentes,
#      rol administrador y credenciales expiradas para pruebas de seguridad.
#   2. Archivos de imagen en formato PNG calibrados para los diferentes escalones
#      de tamaño estipulados por la dimensión V1 (1 KB, 1 MB, 10 MB, 50 MB).
#   3. Cargas maliciosas o malformadas (imágenes con encabezados corruptos y archivos
#      de texto plano) para validar la robustez y el rechazo con HTTP 422.
# ==============================================================================

# 1. Módulos de la biblioteca estándar de Python para sistema operativo y rutas
import os
import sys
from pathlib import Path

# 2. Módulos de serialización y codificación para generación de JWTs
import json
import base64
import hmac
import hashlib
import time

# ==============================================================================
# Definición de Directorios de Salida
# ==============================================================================

# Directorio donde reside este script (hit3/carga/)
DIR_BASE = Path(__file__).resolve().parent

# Subdirectorio donde se guardarán los tokens JSON y las imágenes sintéticas generadas
DIR_DATOS = DIR_BASE / "datos"


# ==============================================================================
# Función: Obtención del Secreto Compartido HMAC-SHA256
# ==============================================================================
def obtener_secreto() -> str:
    """
    Obtiene la clave secreta compartida utilizada para firmar y validar tokens JWT.
    
    Estrategia de resolución en cascada:
    1. Variable de entorno 'SOBEL_SECRETO' (prioridad máxima, útil en CI/CD o Kubernetes).
    2. Archivo .env.secrets en la carpeta local, en hit3/ o en la raíz del repositorio
       (generado de forma segura mediante crear_secretos.sh).
    3. Valor de contingencia por defecto si no se halla configuración previa.
    """
    # 1. Verificar si la variable de entorno ya está presente
    if "SOBEL_SECRETO" in os.environ and os.environ["SOBEL_SECRETO"]:
        return os.environ["SOBEL_SECRETO"]
    
    # 2. Rutas candidatas de búsqueda para archivos de secretos locales
    candidatos = [
        DIR_BASE / ".env.secrets",
        DIR_BASE.parent / ".env.secrets",
        DIR_BASE.parent.parent / ".env.secrets",
    ]
    
    # Iterar sobre las rutas buscando la clave SOBEL_SECRETO
    for p in candidatos:
        if p.exists():
            try:
                for line in p.read_text(encoding="utf-8").splitlines():
                    # Parsear líneas clave=valor ignorando comentarios
                    if line.startswith("SOBEL_SECRETO="):
                        val = line.split("=", 1)[1].strip()
                        if val:
                            return val
            except Exception:
                pass
    
    # 3. Valor fallback de desarrollo si no existe secreto configurado
    return "secreto-sobel-super-seguro"


# Secreto por defecto inicializado al cargar el módulo
SOBEL_SECRETO_DEFAULT = obtener_secreto()


# ==============================================================================
# Función Auxiliar: Codificación Base64URL conforme a RFC 7519
# ==============================================================================
def b64url(data: bytes) -> str:
    """
    Codifica un flujo de bytes en formato Base64URL seguro para URLs.
    
    Reglas del estándar JWT (RFC 7519):
    - Sustituye '+' por '-' y '/' por '_'.
    - Elimina los caracteres de relleno '=' al final de la cadena codificada.
    """
    # Aplica urlsafe_b64encode, recorta los signos '=' finales y decodifica a UTF-8
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


# ==============================================================================
# Función: Emisión de Tokens JWT HS256 sin Dependencias Externas
# ==============================================================================
def emitir_jwt(sub: str, role: str, secret: str, ttl_s: int = 3600, exp: int = None) -> str:
    """
    Genera un token JWT firmado compatible con el middleware de autenticación del backend.
    
    Parámetros:
    - sub: Identificador del sujeto / usuario (ej: 'user_01', 'admin_k6').
    - role: Rol asignado en el sistema ('usuario' o 'admin').
    - secret: Clave simétrica compartida para el algoritmo HS256.
    - ttl_s: Tiempo de vida en segundos a partir de ahora (default: 3600s = 1 hora).
    - exp: Timestamp UNIX explícito de expiración (para generar tokens deliberadamente vencidos).
    
    Estructura generada:
    base64url(header) + "." + base64url(payload) + "." + base64url(hmac_sha256(header.payload))
    """
    # 1. Encabezado indicando el algoritmo de firma simétrica HMAC-SHA256
    header = {"alg": "HS256", "typ": "JWT"}
    
    # 2. Carga útil (claims obligatorios según la Sección 3 del Contrato de la API)
    payload = {
        "sub": sub,
        "role": role,
        "exp": int(time.time()) + ttl_s if exp is None else exp
    }
    
    # 3. Serialización compacta en JSON (sin espacios superfluos para consistencia)
    h_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
    p_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    
    # 4. Codificación en Base64URL de encabezado y payload
    h_str = b64url(h_bytes)
    p_str = b64url(p_bytes)
    
    # 5. Construcción del mensaje a firmar: "header.payload"
    mensaje = f"{h_str}.{p_str}".encode("utf-8")
    
    # 6. Cálculo de la firma criptográfica HMAC con SHA-256
    firma = hmac.new(secret.encode("utf-8"), mensaje, hashlib.sha256).digest()
    sig_str = b64url(firma)
    
    # 7. Retorna el token JWT completo ensamblado con sus tres componentes separados por punto
    return f"{h_str}.{p_str}.{sig_str}"


# ==============================================================================
# Función: Generación del Conjunto de Tokens de Prueba
# ==============================================================================
def generar_tokens(secret: str = SOBEL_SECRETO_DEFAULT, cant_usuarios: int = 50) -> Path:
    """
    Genera y guarda en disco la base de datos de tokens para la prueba de carga k6.
    
    Genera:
    - Identidades individuales de 'user_01' a 'user_50' (para simular concurrencia real VC).
    - 1 identidad con rol 'admin' (para endpoints privilegiados o monitoreo).
    - 1 token expirado (para verificar el rechazo con HTTP 401 en el smoke test).
    """
    # Estructura de diccionario de tokens
    tokens = {
        "usuarios": {},
        # Token con validez de 24 horas para operaciones de administración
        "admin": emitir_jwt("admin_k6", "admin", secret, ttl_s=86400),
        # Token con timestamp de expiración fijado en el pasado (hace 1 hora)
        "expirado": emitir_jwt("bot_expirado", "usuario", secret, exp=int(time.time()) - 3600),
    }

    # Bucle para emitir credenciales independientes para cada uno de los 50 usuarios virtuales
    for i in range(1, cant_usuarios + 1):
        user_name = f"user_{i:02d}"
        tokens["usuarios"][user_name] = emitir_jwt(user_name, "usuario", secret, ttl_s=86400)

    # Ruta de destino dentro del subdirectorio datos/
    archivo_tokens = DIR_DATOS / "tokens.json"
    with open(archivo_tokens, "w", encoding="utf-8") as f:
        # Serializa con sangría legible para facilitar inspección y depuración
        json.dump(tokens, f, indent=2)

    print(f"[OK] Generados {cant_usuarios} tokens de usuario, 1 admin y 1 expirado en: {archivo_tokens}")
    return archivo_tokens


# ==============================================================================
# Función: Generación de Imágenes Sintéticas Calibradas
# ==============================================================================
def generar_imagenes():
    """
    Genera imágenes sintéticas en formato PNG calibradas para los diferentes pesos
    de la dimensión V1 requeridos en la Matriz R5 del TP3:
      - V1 = 1kb  -> Imagen diminuta de 32x32 píxeles.
      - V1 = 1mb  -> Imagen mediana de 600x600 píxeles.
      - V1 = 10mb -> Imagen pesada de 1850x1850 píxeles.
      - V1 = 50mb -> Imagen masiva de 4150x4150 píxeles (con compresión rápida).
      - Imagen corrupta -> Archivo con extensión .png pero con cabecera binaria inválida.
      - Archivo de texto -> Archivo de texto plano para probar tipo MIME erróneo.
    """
    # Verificación de librerías para generación de imágenes sintéticas
    try:
        from PIL import Image
        import numpy as np
    except ImportError:
        print("[ERROR] Se requieren las librerías 'PIL' (Pillow) y 'numpy' para sintetizar imágenes.")
        print("Instalar mediante: pip install pillow numpy")
        sys.exit(1)

    # Asegura la existencia física del directorio de salida
    DIR_DATOS.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------------------
    # 1. Imagen ~1 KB: 32x32 píxeles RGB con entropía pseudo-aleatoria
    # --------------------------------------------------------------------------
    path_1k = DIR_DATOS / "img_1kb.png"
    arr_1k = np.random.randint(0, 256, (32, 32, 3), dtype=np.uint8)
    Image.fromarray(arr_1k).save(path_1k, format="PNG")
    print(f"[OK] Creada {path_1k.name} ({path_1k.stat().st_size} bytes)")

    # --------------------------------------------------------------------------
    # 2. Imagen ~1 MB: 600x600 píxeles RGB con ruido uniforme
    # --------------------------------------------------------------------------
    path_1m = DIR_DATOS / "img_1mb.png"
    arr_1m = np.random.randint(0, 256, (600, 600, 3), dtype=np.uint8)
    Image.fromarray(arr_1m).save(path_1m, format="PNG")
    print(f"[OK] Creada {path_1m.name} ({path_1m.stat().st_size / 1024 / 1024:.2f} MB)")

    # --------------------------------------------------------------------------
    # 3. Imagen ~10 MB: 1850x1850 píxeles RGB
    # --------------------------------------------------------------------------
    path_10m = DIR_DATOS / "img_10mb.png"
    arr_10m = np.random.randint(0, 256, (1850, 1850, 3), dtype=np.uint8)
    Image.fromarray(arr_10m).save(path_10m, format="PNG")
    print(f"[OK] Creada {path_10m.name} ({path_10m.stat().st_size / 1024 / 1024:.2f} MB)")

    # --------------------------------------------------------------------------
    # 4. Imagen ~50 MB: 4150x4150 píxeles RGB con nivel de compresión 1
    # --------------------------------------------------------------------------
    # Usamos compress_level=1 para acelerar dramáticamente el guardado en disco
    # (generación en menos de 1 segundo en lugar de 40 segundos).
    path_50m = DIR_DATOS / "img_50mb.png"
    arr_50m = np.random.randint(0, 256, (4150, 4150, 3), dtype=np.uint8)
    Image.fromarray(arr_50m).save(path_50m, format="PNG", compress_level=1)
    print(f"[OK] Creada {path_50m.name} ({path_50m.stat().st_size / 1024 / 1024:.2f} MB)")

    # --------------------------------------------------------------------------
    # 5. Imagen deliberadamente corrupta: cabecera inválida
    # --------------------------------------------------------------------------
    # No contiene los bytes mágicos PNG (\\x89PNG\\r\\n\\x1a\\n).
    path_corrupta = DIR_DATOS / "corrupta.png"
    with open(path_corrupta, "wb") as f:
        f.write(b"NOT_A_PNG_FILE_HEADER_" + os.urandom(1024))
    print(f"[OK] Creada {path_corrupta.name} (archivo malformado para verificar HTTP 422)")

    # --------------------------------------------------------------------------
    # 6. Archivo de texto plano con extensión incorrecta
    # --------------------------------------------------------------------------
    path_txt = DIR_DATOS / "no_imagen.txt"
    with open(path_txt, "w", encoding="utf-8") as f:
        f.write("Este no es un archivo de imagen válido.")
    print(f"[OK] Creada {path_txt.name} (archivo de texto plano)")


# ==============================================================================
# Punto de Entrada Principal (Ejecución CLI)
# ==============================================================================
def main():
    """Ejecuta secuencialmente la preparación del directorio y la generación de tokens e imágenes."""
    # Crea el subdirectorio ./datos si no existe
    DIR_DATOS.mkdir(parents=True, exist_ok=True)
    
    # Obtiene la clave simétrica en uso para la firma
    secreto = os.environ.get("SOBEL_SECRETO", SOBEL_SECRETO_DEFAULT)
    
    print("=== Generando datos sintéticos para pruebas de carga k6 ===")
    print(f"Directorio de salida: {DIR_DATOS}")
    print(f"Secreto JWT en uso: {secreto[:4]}***")
    
    # 1. Generar 50 identidades de usuario + admin + expirado
    generar_tokens(secret=secreto, cant_usuarios=50)
    
    # 2. Generar imágenes en disco calibradas a 1KB, 1MB, 10MB y 50MB
    generar_imagenes()
    
    print("=== Datos de prueba listos exitosamente ===")


# Invoca la función principal si el script se llama de forma directa
if __name__ == "__main__":
    main()
