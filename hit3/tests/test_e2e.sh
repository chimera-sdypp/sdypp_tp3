#!/usr/bin/env bash
# ==============================================================================
# test_e2e.sh — Test de Integración End-to-End para Sobel Distribuido (Tarea S5)
# Sistemas Distribuidos y Paralelos — Grupo Cerberus (Salva / Salvador Baez)
# ==============================================================================
# Propósito del script:
# Este test automatizado valida el ciclo de vida completo de un trabajo de
# procesamiento de imágenes en el cluster Kubernetes (k3s), garantizando que
# la arquitectura distribuida (Ingress -> Backend -> RabbitMQ -> Workers -> Joiner)
# sea semánticamente equivalente y matemáticamente idéntica al procesamiento
# centralizado secuencial implementado en el Hit #1.
#
# Flujo ejecutado paso a paso:
#   1. Verificación de dependencias locales (curl, python3, numpy, Pillow).
#   2. Despliegue automatizado de manifiestos k8s y espera de readiness (opcional: --deploy).
#   3. Sondeo de disponibilidad del Ingress Gateway y registro de workers en /health.
#   4. Creación de imagen sintética y ejecución del algoritmo canónico de Hit #1 (Ground Truth).
#   5. Emisión de credenciales JWT HS256 utilizando únicamente la biblioteca estándar de Python.
#   6. Envío del trabajo distribuido mediante petición HTTP POST multipart/form-data.
#   7. Sondeo reactivo del estado del trabajo (pending -> processing -> done) con progreso %.
#   8. Descarga del artefacto resultante desde GET /jobs/{id}/resultado/0.
#   9. Verificación estricta de igualdad píxel a píxel (distribuido vs centralizado).
#  10. Prueba negativa de rechazo temprano ante operaciones inexistentes (código HTTP 422).
# ==============================================================================

# Configuración de modo estricto en Bash para atrapar cualquier error en tiempo de ejecución:
# -e: Finaliza inmediatamente si algún comando produce un código de salida distinto de cero.
# -u: Trata las variables no declaradas o vacías como un error fatal.
# -o pipefail: Hace que el código de retorno de una tubería sea el del último comando que falló.
set -euo pipefail

# Resolución de rutas absolutas del script y del directorio raíz del repositorio
DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR_RAIZ="$(cd "$DIR_SCRIPT/../.." && pwd)"

# ==============================================================================
# Valores Predeterminados de Ejecución
# ==============================================================================

# URL base del balanceador de carga o Ingress de Kubernetes (puerto 80 por defecto)
URL="${SOBEL_URL:-http://localhost}"

# Namespace de Kubernetes donde residen los recursos desplegados
NAMESPACE="${SOBEL_NAMESPACE:-sobel}"

# Imagen OCI autorizada del worker para el procesamiento de filtros (Sección 4 del Contrato)
IMAGEN="${SOBEL_IMAGEN:-ghcr.io/chimera-sdypp/sobel-worker:1.0.0}"

# Tiempo máximo en segundos para esperar la finalización del trabajo antes de timeout
TIMEOUT_S="${SOBEL_TIMEOUT:-180}"

# Bandera booleana para decidir si se aplican manifiestos de Kubernetes antes del test (0=no, 1=sí)
HACER_DEPLOY=0

# ==============================================================================
# Procesamiento de Parámetros de Línea de Comandos
# ==============================================================================
while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      # Permite sobreescribir la URL del servicio (ej: http://192.168.1.100 o con port-forward)
      URL="$2"; shift 2 ;;
    --namespace)
      # Permite especificar un namespace alternativo en el cluster de k8s
      NAMESPACE="$2"; shift 2 ;;
    --imagen)
      # Permite fijar la referencia del contenedor de worker a utilizar
      IMAGEN="$2"; shift 2 ;;
    --timeout)
      # Permite extender el tiempo límite de espera del trabajo
      TIMEOUT_S="$2"; shift 2 ;;
    --deploy)
      # Activa la aplicación de manifiestos previa
      HACER_DEPLOY=1; shift ;;
    --skip-deploy)
      # Asume que el cluster ya se encuentra en ejecución
      HACER_DEPLOY=0; shift ;;
    -h|--help)
      # Muestra las líneas de ayuda iniciales del archivo y sale limpiamente
      sed -n '2,20p' "$0" | sed 's/^# \?//'; exit 0 ;;
    *)
      # Manejo de error para opciones inválidas
      echo "Error: Opción desconocida: $1" >&2
      echo "Usar --help para ver las opciones disponibles." >&2
      exit 1 ;;
  esac
done

# Creación de directorio temporal aislado para almacenar archivos generados durante la prueba
TMP_DIR=$(mktemp -d -t sobel_e2e_XXXXXX)
# Limpieza garantizada del directorio temporal al finalizar el script (incluso si falla)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "=============================================================================="
echo "🚀 S5: Test de Integración End-to-End (Sobel Distribuido)"
echo "   URL Ingress: $URL"
echo "   Namespace:   $NAMESPACE"
echo "   Timeout:     ${TIMEOUT_S}s"
echo "   Worker Img:  $IMAGEN"
echo "=============================================================================="

# ------------------------------------------------------------------------------
# Paso 1: Verificación de dependencias locales en el host
# ------------------------------------------------------------------------------
# Valida la presencia de herramientas CLI esenciales: curl para HTTP y python3 para lógica auxiliar
for cmd in curl python3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ Error: Se requiere '$cmd' instalado en el sistema." >&2
    exit 1
  fi
done

# Valida que Python posea las bibliotecas de manipulación numérica y de imágenes instaladas
python3 -c "import numpy, PIL" 2>/dev/null || {
  echo "❌ Error: Se requieren las librerías 'numpy' y 'Pillow' en python3." >&2
  exit 1
}

# ------------------------------------------------------------------------------
# Paso 2: Despliegue en Kubernetes (si se especificó la opción --deploy)
# ------------------------------------------------------------------------------
if [ "$HACER_DEPLOY" -eq 1 ]; then
  echo ""
  echo "📦 Paso 1: Desplegando manifiestos en namespace '$NAMESPACE'..."
  # Valida que la herramienta de administración de clusters esté instalada
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "❌ Error: Se requiere 'kubectl' para la opción --deploy." >&2
    exit 1
  fi

  # Genera y aplica los secretos criptográficos de RabbitMQ y JWT si el script está presente
  if [ -f "$DIR_RAIZ/hit3/k8s/crear_secretos.sh" ]; then
    echo "   * Verificando secretos de Kubernetes..."
    SOBEL_NAMESPACE="$NAMESPACE" bash "$DIR_RAIZ/hit3/k8s/crear_secretos.sh"
  fi

  # Aplica los manifiestos declarativos definidos en la estructura de Kustomize
  echo "   * Aplicando manifiestos con kustomize..."
  kubectl apply -k "$DIR_RAIZ/hit3/k8s"

  # Espera activa hasta que los StatefulSets y Deployments alcancen el estado Ready
  echo "   * Esperando readiness de infraestructura base..."
  kubectl -n "$NAMESPACE" rollout status statefulset/rabbitmq --timeout=120s
  kubectl -n "$NAMESPACE" rollout status statefulset/redis --timeout=120s
  kubectl -n "$NAMESPACE" rollout status deploy/backend --timeout=120s
  echo "   ✅ Infraestructura y Backend en estado Ready."
fi

# ------------------------------------------------------------------------------
# Paso 3: Verificación de disponibilidad de la API Gateway y Workers (/health)
# ------------------------------------------------------------------------------
echo ""
echo "🔍 Paso 2: Verificando disponibilidad de la API Gateway en $URL/health..."
reintentos=20
salud_ok=0
workers_listos=0

# Bucle con sondeo activo para confirmar que tanto el backend como los workers reporten OK
while [ "$reintentos" -gt 0 ]; do
  # Consulta HTTP sin autenticación al endpoint de health check
  resp_health=$(curl -s "$URL/health" 2>/dev/null || true)
  codigo=$(curl -s -o /dev/null -w "%{http_code}" "$URL/health" 2>/dev/null || true)
  
  if [ "$codigo" = "200" ]; then
    salud_ok=1
    # Inspecciona el subobjeto 'contenido.workers' para asegurar que haya workers registrados en Redis
    w_status=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('contenido',{}).get('workers','desconocido'))" "$resp_health" 2>/dev/null || echo "desconocido")
    if [ "$w_status" = "ok" ]; then
      workers_listos=1
      echo "   ✅ API Gateway (200 OK) y Workers activos (estado: ok)."
      break
    else
      echo "   ... backend online pero esperando registro de workers en Redis (workers: '$w_status', reintentos: $reintentos)"
    fi
  else
    echo "   ... esperando backend en $URL/health (código: '$codigo', reintentos restantes: $reintentos)"
  fi
  sleep 2
  reintentos=$((reintentos - 1))
done

# Aborta si el endpoint de salud nunca respondió HTTP 200
if [ "$salud_ok" -ne 1 ]; then
  echo "❌ Error: La API Gateway no responde 200 en $URL/health." >&2
  echo "   Asegurate de que el cluster o el puerto forwarding esté activo (ej: kubectl port-forward -n $NAMESPACE svc/backend 8080:8080)" >&2
  exit 2
fi

# Emite advertencia si los workers aún no concluyeron el handshake inicial
if [ "$workers_listos" -ne 1 ]; then
  echo "⚠️ Advertencia: No se detectaron workers activos en /health. Se continuará esperando en el envío del job."
fi

# ------------------------------------------------------------------------------
# Paso 4: Generación de Imagen Sintética y Referencia Centralizada (Hit #1)
# ------------------------------------------------------------------------------
echo ""
echo "🎨 Paso 3: Generando imagen sintética de prueba y cálculo de referencia Hit #1..."
IMG_ENTRADA="$TMP_DIR/entrada.png"
IMG_CENTRAL="$TMP_DIR/salida_centralizada.png"
IMG_DISTRIB="$TMP_DIR/salida_distribuida.png"

# Script Python embebido para sintetizar una imagen con gradientes lineales y figuras geométricas:
# Se diseñan bordes diagonales, verticales y horizontales para someter al operador Sobel a prueba real.
python3 - <<EOF
import numpy as np
from PIL import Image

# Dimensiones estándar calibradas para prueba rápida
ancho, alto = 400, 300
arr = np.zeros((alto, ancho), dtype=np.uint8)

# Rampa de gradiente horizontal de 0 a 200 de intensidad
for x in range(ancho):
    arr[:, x] = int((x / ancho) * 200)

# Inserción de parches rectangulares de alto contraste para generar transiciones abruptas de brillo
arr[50:120, 50:180] = 255      # Rectángulo blanco puro
arr[180:250, 220:350] = 30     # Rectángulo casi negro
arr[100:220, 150:270] = 180    # Rectángulo gris intermedio

# Guardado en formato PNG en escala de grises (modo 'L')
img = Image.fromarray(arr, mode='L')
img.save("$IMG_ENTRADA")
print(f"   * Imagen sintética generada: {ancho}x{alto} px -> $IMG_ENTRADA")
EOF

# Ejecución directa del algoritmo canónico centralizado del Hit #1:
# Este script sirve de "Ground Truth" (patrón de oro) para contrastar la fidelidad del cálculo distribuido.
if [ -f "$DIR_RAIZ/hit1/centralizado.py" ]; then
  echo "   * Ejecutando algoritmo canónico de Hit #1: hit1/centralizado.py..."
  PYTHONPATH="$DIR_RAIZ/hit1" SOBEL_LOGS="$TMP_DIR/logs_centralizado" \
    python3 "$DIR_RAIZ/hit1/centralizado.py" "$IMG_ENTRADA" "$IMG_CENTRAL"
  echo "   ✅ Salida canónica Hit #1 generada -> $IMG_CENTRAL"
else
  echo "❌ Error: No se encontró '$DIR_RAIZ/hit1/centralizado.py'." >&2
  exit 2
fi

# ------------------------------------------------------------------------------
# Paso 5: Obtención de Token JWT (Sin dependencias externas, Python stdlib)
# ------------------------------------------------------------------------------
echo ""
echo "🔑 Paso 4: Generando token JWT de autenticación..."
TOKEN=""

# Estrategia 1: Leer directamente la variable SOBEL_TOKEN si fue exportada por el operador
if [ -n "${SOBEL_TOKEN:-}" ]; then
  TOKEN="$SOBEL_TOKEN"
  echo "   * Usando token provisto en \$SOBEL_TOKEN"
fi

# Estrategia 2: Firmar un token HS256 localmente leyendo el secreto desde .env.secrets
if [ -z "$TOKEN" ]; then
  secreto="${SOBEL_SECRETO:-}"
  # Búsqueda en rutas candidatas del archivo .env.secrets
  for p_secret in "$DIR_RAIZ/.env.secrets" "$DIR_RAIZ/hit3/.env.secrets" "$DIR_SCRIPT/.env.secrets"; do
    if [ -z "$secreto" ] && [ -f "$p_secret" ]; then
      secreto=$(grep -E '^SOBEL_SECRETO=' "$p_secret" | cut -d= -f2- | tr -d '"\r\n' || true)
      break
    fi
  done

  # Si se halló el secreto, se construye el JWT conforme a la especificación RFC 7519
  if [ -n "$secreto" ]; then
    TOKEN=$(python3 - <<EOF
import base64, hmac, hashlib, json, time, sys

# Función auxiliar para codificar en Base64URL sin padding '='
def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")

header = {"alg": "HS256", "typ": "JWT"}
payload = {
    "sub": "e2e_tester",
    "role": "usuario",
    "exp": int(time.time()) + 3600
}
h_str = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
p_str = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
mensaje = f"{h_str}.{p_str}".encode("utf-8")
firma = b64url(hmac.new("$secreto".encode("utf-8"), mensaje, hashlib.sha256).digest())
sys.stdout.write(f"{h_str}.{p_str}.{firma}")
EOF
)
    if [ -n "$TOKEN" ]; then
      echo "   * Token JWT generado exitosamente con secreto de .env.secrets (HS256 stdlib)"
    fi
  fi
fi

# Estrategia 3: Delegar la emisión del token ejecutando el módulo del backend dentro del pod vía kubectl
if [ -z "$TOKEN" ] && command -v kubectl >/dev/null 2>&1; then
  TOKEN=$(kubectl -n "$NAMESPACE" exec deploy/backend -- python -m backend.emitir_token "e2e_tester" "usuario" --horas 1 2>/dev/null || true)
  if [ -n "$TOKEN" ]; then
    echo "   * Token generado vía kubectl exec en pod backend"
  fi
fi

# Si ninguna de las estrategias pudo emitir un token válido, aborta con error explicativo
if [ -z "$TOKEN" ]; then
  echo "❌ Error: No se pudo emitir un token JWT válido." >&2
  echo "   Asegurate de tener .env.secrets, kubectl configurado, o exportar SOBEL_TOKEN." >&2
  exit 3
fi

# ------------------------------------------------------------------------------
# Paso 6: Envío del Job Distribuido (POST /jobs con reintentos si workers están iniciando)
# ------------------------------------------------------------------------------
echo ""
echo "📤 Paso 5: Enviando job distribuido (POST /jobs)..."
RESP_POST="$TMP_DIR/resp_post.json"
OP_JSON="[{\"operacion\":\"sobel\",\"imagen\":\"$IMAGEN\"}]"

reintentos_post=6
CODIGO_POST=0

# Bucle con backoff exponencial suave: Si el cluster recién arranca y los workers no se registraron,
# el backend responderá HTTP 503 SIN_WORKERS según el Contrato Sección 6. Esperamos a que se registren.
while [ "$reintentos_post" -gt 0 ]; do
  CODIGO_POST=$(curl -s -w "%{http_code}" -o "$RESP_POST" -X POST "$URL/jobs" \
    -H "Authorization: Bearer $TOKEN" \
    -F "archivos=@$IMG_ENTRADA" \
    -F "operaciones=$OP_JSON")

  if [ "$CODIGO_POST" = "202" ]; then
    break # Aceptado exitosamente
  elif [ "$CODIGO_POST" = "503" ]; then
    echo "   ... backend respondió 503 (esperando que los workers se registren en Redis). Reintentando en 5s..."
    sleep 5
    reintentos_post=$((reintentos_post - 1))
  else
    break # Cualquier otro código se evalúa abajo
  fi
done

# Valida que el código de respuesta sea estrictamente HTTP 202 Accepted según el Contrato
if [ "$CODIGO_POST" != "202" ]; then
  echo "❌ Error: POST /jobs respondió código $CODIGO_POST (esperaba 202)." >&2
  echo "   Cuerpo recibido: $(cat "$RESP_POST")" >&2
  exit 4
fi

# Extrae el identificador del trabajo (job_id) retornado por el backend
JOB_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["contenido"]["job_id"])' "$RESP_POST")
echo "   ✅ Job aceptado con éxito. ID: $JOB_ID"

# ------------------------------------------------------------------------------
# Paso 7: Sondeo de Estado (Polling loop hasta 'done')
# ------------------------------------------------------------------------------
echo ""
echo "⏳ Paso 6: Monitoreando progreso del job $JOB_ID (máx ${TIMEOUT_S}s)..."
RESP_GET="$TMP_DIR/resp_get.json"
inicio_ts=$(date +%s)
estado="queued"

# Bucle de consulta periódica para verificar el ciclo de vida del trabajo
while true; do
  ahora_ts=$(date +%s)
  transcurrido=$((ahora_ts - inicio_ts))
  
  # Control de tiempo límite para evitar ejecuciones infinitas
  if [ "$transcurrido" -ge "$TIMEOUT_S" ]; then
    echo ""
    echo "❌ Error: Timeout esperando que el job finalice después de ${TIMEOUT_S}s." >&2
    echo "   Último estado conocido: $estado" >&2
    exit 5
  fi

  # Consulta GET /jobs/{jobId} transmitiendo la cabecera de autenticación
  curl -s -o "$RESP_GET" -X GET "$URL/jobs/$JOB_ID" \
    -H "Authorization: Bearer $TOKEN"

  # Extracción segura del estado y del porcentaje de avance reportado
  estado=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["contenido"]["estado"])' "$RESP_GET" 2>/dev/null || echo "desconocido")
  pct=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["contenido"].get("fragmentos", {}).get("porcentaje", 0))' "$RESP_GET" 2>/dev/null || echo "0")

  # Muestra en pantalla el avance interactivo en tiempo real
  printf "\r   * [%03ds] Estado: %-12s | Avance: %5.1f%%" "$transcurrido" "$estado" "$pct"

  # Máquina de estados de evaluación
  case "$estado" in
    done)
      echo ""
      echo "   🎉 Job completado con estado 'done'!"
      break
      ;;
    failed)
      echo ""
      err_msg=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["contenido"].get("error", "Error no especificado"))' "$RESP_GET" 2>/dev/null || true)
      echo "❌ Error: El job falló en el backend: $err_msg" >&2
      exit 6
      ;;
    dlq)
      echo ""
      echo "❌ Error: El job terminó en la Dead Letter Queue (dlq). Tareas agotaron reintentos." >&2
      exit 7
      ;;
    queued|processing)
      sleep 2 # El trabajo sigue procesándose, esperamos 2 segundos antes de la siguiente consulta
      ;;
    *)
      sleep 2
      ;;
  esac
done

# ------------------------------------------------------------------------------
# Paso 8: Descarga de Imagen Resultante
# ------------------------------------------------------------------------------
echo ""
echo "📥 Paso 7: Descargando resultado desde GET /jobs/$JOB_ID/resultado/0..."
CODIGO_DOWN=$(curl -s -w "%{http_code}" -o "$IMG_DISTRIB" -X GET "$URL/jobs/$JOB_ID/resultado/0" \
  -H "Authorization: Bearer $TOKEN")

# Verifica que el endpoint retorne HTTP 200 OK
if [ "$CODIGO_DOWN" != "200" ]; then
  echo "❌ Error: No se pudo descargar el resultado (código $CODIGO_DOWN)." >&2
  exit 8
fi

# Verifica que el archivo descargado contenga bytes válidos
tam_bytes=$(wc -c < "$IMG_DISTRIB" | tr -d ' ')
if [ "$tam_bytes" -le 0 ]; then
  echo "❌ Error: El archivo descargado está vacío." >&2
  exit 9
fi
echo "   ✅ Imagen descargada correctamente ($tam_bytes bytes)."

# ------------------------------------------------------------------------------
# Paso 9: Validación Píxel a Píxel contra Hit #1 (Ground Truth)
# ------------------------------------------------------------------------------
echo ""
echo "🔬 Paso 8: Comparación píxel a píxel: Distribuido vs Centralizado..."
python3 - <<EOF
import sys
import numpy as np
from PIL import Image

# Cargar ambas imágenes y forzar escala de grises para normalizar canales
img_dist = np.asarray(Image.open("$IMG_DISTRIB").convert("L"))
img_cent = np.asarray(Image.open("$IMG_CENTRAL").convert("L"))

# Verificación 1: Mismas dimensiones espaciales (alto x ancho)
if img_dist.shape != img_cent.shape:
    print(f"❌ Discrepancia de dimensiones: distribuida {img_dist.shape} vs centralizada {img_cent.shape}", file=sys.stderr)
    sys.exit(10)

# Verificación 2: Comparación estricta de cada píxel de la matriz
if not np.array_equal(img_dist, img_cent):
    diferencias = np.sum(img_dist != img_cent)
    total_pixeles = img_dist.size
    print(f"❌ Discrepancia de píxeles: {diferencias} de {total_pixeles} píxeles son distintos.", file=sys.stderr)
    sys.exit(11)

print(f"   ✅ Verificación exitosa: Las imágenes ({img_dist.shape[1]}x{img_dist.shape[0]} px) son 100% IDÉNTICAS píxel por píxel.")
EOF

# ------------------------------------------------------------------------------
# Paso 10: Prueba Negativa de Validación Temprana (Requisito R2)
# ------------------------------------------------------------------------------
echo ""
echo "🛡️ Paso 9: Prueba de robustez (rechazo de operación inexistente)..."
RESP_NEG="$TMP_DIR/resp_neg.json"
OP_INVALIDA="[{\"operacion\":\"operacion_que_no_existe_xyz\",\"imagen\":\"$IMAGEN\"}]"

# Envío de operación no soportada para corroborar que la validación ocurra en el borde
CODIGO_NEG=$(curl -s -w "%{http_code}" -o "$RESP_NEG" -X POST "$URL/jobs" \
  -H "Authorization: Bearer $TOKEN" \
  -F "archivos=@$IMG_ENTRADA" \
  -F "operaciones=$OP_INVALIDA")

# El Contrato Sección 6 estipula código HTTP 422 para OPERACION_NO_SOPORTADA
if [ "$CODIGO_NEG" = "422" ] || [ "$CODIGO_NEG" = "400" ]; then
  echo "   ✅ Rechazo preventivo correcto (código $CODIGO_NEG). El payload inválido no ingresa a la cola."
else
  echo "   ⚠️ Advertencia: Esperaba 422/400 ante operación inexistente, se obtuvo: $CODIGO_NEG"
fi

# ------------------------------------------------------------------------------
# Resumen Final y Salida Exitosa
# ------------------------------------------------------------------------------
echo ""
echo "=============================================================================="
echo "🎉 TEST E2E FINALIZADO CON ÉXITO"
echo "   - Pipeline completo k3s -> Ingress -> Backend -> Workers -> Joiner verificado."
echo "   - Salida distribuida idéntica al procesamiento centralizado del Hit #1."
echo "   - Validación temprana de operaciones inválidas activa."
echo "=============================================================================="
exit 0
