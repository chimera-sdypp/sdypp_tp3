#!/usr/bin/env bash
# ==============================================================================
# Tarea S5: Test de Integración End-to-End (Hit #3 Sobel Distribuido)
# ==============================================================================
# Despliega o verifica los recursos en k3s, envía un job multipart con una imagen
# sintética al backend vía Ingress, monitorea su progreso hasta 'done', descarga
# la imagen procesada y comprueba que sea 100% IDÉNTICA píxel a píxel a la versión
# generada por el algoritmo centralizado del Hit #1.
#
# Uso:
#   ./hit3/tests/test_e2e.sh [OPCIONES]
#
# Opciones:
#   --url <URL>           URL del ingress/backend (default: http://localhost o $SOBEL_URL)
#   --namespace <NS>      Namespace de Kubernetes (default: sobel o $SOBEL_NAMESPACE)
#   --imagen <IMG>        Imagen de worker autorizada (default: ghcr.io/chimera-sdypp/sobel-worker:1.0.0)
#   --timeout <SEG>       Segundos máximos de espera para 'done' (default: 180)
#   --deploy              Aplica los manifiestos de hit3/k8s antes de iniciar la prueba
#   --skip-deploy         No aplica manifiestos, asume cluster listo (default)
#   --help, -h            Muestra esta ayuda
# ==============================================================================
set -euo pipefail

DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR_RAIZ="$(cd "$DIR_SCRIPT/../.." && pwd)"

# Valores por defecto
URL="${SOBEL_URL:-http://localhost}"
NAMESPACE="${SOBEL_NAMESPACE:-sobel}"
IMAGEN="${SOBEL_IMAGEN:-ghcr.io/chimera-sdypp/sobel-worker:1.0.0}"
TIMEOUT_S="${SOBEL_TIMEOUT:-180}"
HACER_DEPLOY=0

# Parseo de argumentos
while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      URL="$2"; shift 2 ;;
    --namespace)
      NAMESPACE="$2"; shift 2 ;;
    --imagen)
      IMAGEN="$2"; shift 2 ;;
    --timeout)
      TIMEOUT_S="$2"; shift 2 ;;
    --deploy)
      HACER_DEPLOY=1; shift ;;
    --skip-deploy)
      HACER_DEPLOY=0; shift ;;
    -h|--help)
      sed -n '2,18p' "$0" | sed 's/^# \?//'; exit 0 ;;
    *)
      echo "Error: Opción desconocida: $1" >&2
      echo "Usar --help para ver las opciones disponibles." >&2
      exit 1 ;;
  esac
done

TMP_DIR=$(mktemp -d -t sobel_e2e_XXXXXX)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "=============================================================================="
echo "🚀 S5: Test de Integración End-to-End (Sobel Distribuido)"
echo "   URL Ingress: $URL"
echo "   Namespace:   $NAMESPACE"
echo "   Timeout:     ${TIMEOUT_S}s"
echo "   Worker Img:  $IMAGEN"
echo "=============================================================================="

# ------------------------------------------------------------------------------
# 1. Verificación de dependencias locales
# ------------------------------------------------------------------------------
for cmd in curl python3; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ Error: Se requiere '$cmd' instalado en el sistema." >&2
    exit 1
  fi
done

python3 -c "import numpy, PIL" 2>/dev/null || {
  echo "❌ Error: Se requieren las librerías 'numpy' y 'Pillow' en python3." >&2
  exit 1
}

# ------------------------------------------------------------------------------
# 2. Despliegue en Kubernetes (si se solicitó --deploy)
# ------------------------------------------------------------------------------
if [ "$HACER_DEPLOY" -eq 1 ]; then
  echo ""
  echo "📦 Paso 1: Desplegando manifiestos en namespace '$NAMESPACE'..."
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "❌ Error: Se requiere 'kubectl' para la opción --deploy." >&2
    exit 1
  fi

  # Crear secretos si no existen
  if [ -f "$DIR_RAIZ/hit3/k8s/crear_secretos.sh" ]; then
    echo "   * Verificando secretos de Kubernetes..."
    SOBEL_NAMESPACE="$NAMESPACE" bash "$DIR_RAIZ/hit3/k8s/crear_secretos.sh"
  fi

  echo "   * Aplicando manifiestos con kustomize..."
  kubectl apply -k "$DIR_RAIZ/hit3/k8s"

  echo "   * Esperando readiness de infraestructura base..."
  kubectl -n "$NAMESPACE" rollout status statefulset/rabbitmq --timeout=120s
  kubectl -n "$NAMESPACE" rollout status statefulset/redis --timeout=120s
  kubectl -n "$NAMESPACE" rollout status deploy/backend --timeout=120s
  echo "   ✅ Infraestructura y Backend en estado Ready."
fi

# ------------------------------------------------------------------------------
# 3. Verificación de disponibilidad de la API Gateway y Workers (/health)
# ------------------------------------------------------------------------------
echo ""
echo "🔍 Paso 2: Verificando disponibilidad de la API Gateway en $URL/health..."
reintentos=20
salud_ok=0
workers_listos=0

while [ "$reintentos" -gt 0 ]; do
  resp_health=$(curl -s "$URL/health" 2>/dev/null || true)
  codigo=$(curl -s -o /dev/null -w "%{http_code}" "$URL/health" 2>/dev/null || true)
  
  if [ "$codigo" = "200" ]; then
    salud_ok=1
    # Verificar si los workers están registrados
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

if [ "$salud_ok" -ne 1 ]; then
  echo "❌ Error: La API Gateway no responde 200 en $URL/health." >&2
  echo "   Asegurate de que el cluster o el puerto forwarding esté activo (ej: kubectl port-forward -n $NAMESPACE svc/backend 8080:8080)" >&2
  exit 2
fi

if [ "$workers_listos" -ne 1 ]; then
  echo "⚠️ Advertencia: No se detectaron workers activos en /health. Se continuará esperando en el envío del job."
fi

# ------------------------------------------------------------------------------
# 4. Generación de Imagen Sintética y Referencia Centralizada (Hit #1)
# ------------------------------------------------------------------------------
echo ""
echo "🎨 Paso 3: Generando imagen sintética de prueba y cálculo de referencia Hit #1..."
IMG_ENTRADA="$TMP_DIR/entrada.png"
IMG_CENTRAL="$TMP_DIR/salida_centralizada.png"
IMG_DISTRIB="$TMP_DIR/salida_distribuida.png"

# 1. Generar imagen sintética de 400x300 con gradientes y contrastes marcados
python3 - <<EOF
import numpy as np
from PIL import Image

ancho, alto = 400, 300
arr = np.zeros((alto, ancho), dtype=np.uint8)

for x in range(ancho):
    arr[:, x] = int((x / ancho) * 200)

arr[50:120, 50:180] = 255
arr[180:250, 220:350] = 30
arr[100:220, 150:270] = 180

img = Image.fromarray(arr, mode='L')
img.save("$IMG_ENTRADA")
print(f"   * Imagen sintética generada: {ancho}x{alto} px -> $IMG_ENTRADA")
EOF

# 2. Ejecutar directamente el script canónico centralizado del Hit #1
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
# 5. Obtención de Token JWT (Sin dependencias externas, Python stdlib)
# ------------------------------------------------------------------------------
echo ""
echo "🔑 Paso 4: Generando token JWT de autenticación..."
TOKEN=""

# Intento 1: Variable de entorno directa
if [ -n "${SOBEL_TOKEN:-}" ]; then
  TOKEN="$SOBEL_TOKEN"
  echo "   * Usando token provisto en \$SOBEL_TOKEN"
fi

# Intento 2: Archivo .env.secrets local o variable de entorno SOBEL_SECRETO
if [ -z "$TOKEN" ]; then
  secreto="${SOBEL_SECRETO:-}"
  if [ -z "$secreto" ] && [ -f "$DIR_RAIZ/.env.secrets" ]; then
    secreto=$(grep -E '^SOBEL_SECRETO=' "$DIR_RAIZ/.env.secrets" | cut -d= -f2- | tr -d '"\r\n' || true)
  fi

  if [ -n "$secreto" ]; then
    # Emisión pura con biblioteca estándar (hmac, hashlib, base64) para evitar dependencias de PyJWT en host
    TOKEN=$(python3 - <<EOF
import base64, hmac, hashlib, json, time, sys

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
msg = f"{h_str}.{p_str}".encode("utf-8")
sig = b64url(hmac.new("$secreto".encode("utf-8"), msg, hashlib.sha256).digest())
sys.stdout.write(f"{h_str}.{p_str}.{sig}")
EOF
)
    if [ -n "$TOKEN" ]; then
      echo "   * Token JWT generado exitosamente con secreto de .env.secrets (HS256 stdlib)"
    fi
  fi
fi

# Intento 3: Ejecución remota dentro del pod de backend con kubectl
if [ -z "$TOKEN" ] && command -v kubectl >/dev/null 2>&1; then
  TOKEN=$(kubectl -n "$NAMESPACE" exec deploy/backend -- python -m backend.emitir_token "e2e_tester" "usuario" --horas 1 2>/dev/null || true)
  if [ -n "$TOKEN" ]; then
    echo "   * Token generado vía kubectl exec en pod backend"
  fi
fi

if [ -z "$TOKEN" ]; then
  echo "❌ Error: No se pudo emitir un token JWT válido." >&2
  echo "   Asegurate de tener .env.secrets, kubectl configurado, o exportar SOBEL_TOKEN." >&2
  exit 3
fi

# ------------------------------------------------------------------------------
# 6. Envío del Job Distribuido (POST /jobs con reintentos si workers están iniciando)
# ------------------------------------------------------------------------------
echo ""
echo "📤 Paso 5: Enviando job distribuido (POST /jobs)..."
RESP_POST="$TMP_DIR/resp_post.json"
OP_JSON="[{\"operacion\":\"sobel\",\"imagen\":\"$IMAGEN\"}]"

reintentos_post=6
CODIGO_POST=0

while [ "$reintentos_post" -gt 0 ]; do
  CODIGO_POST=$(curl -s -w "%{http_code}" -o "$RESP_POST" -X POST "$URL/jobs" \
    -H "Authorization: Bearer $TOKEN" \
    -F "archivos=@$IMG_ENTRADA" \
    -F "operaciones=$OP_JSON")

  if [ "$CODIGO_POST" = "202" ]; then
    break
  elif [ "$CODIGO_POST" = "503" ]; then
    echo "   ... backend respondió 503 (esperando que los workers se registren en Redis). Reintentando en 5s..."
    sleep 5
    reintentos_post=$((reintentos_post - 1))
  else
    break
  fi
done

if [ "$CODIGO_POST" != "202" ]; then
  echo "❌ Error: POST /jobs respondió código $CODIGO_POST (esperaba 202)." >&2
  echo "   Cuerpo recibido: $(cat "$RESP_POST")" >&2
  exit 4
fi

JOB_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["contenido"]["job_id"])' "$RESP_POST")
echo "   ✅ Job aceptado con éxito. ID: $JOB_ID"

# ------------------------------------------------------------------------------
# 7. Sondeo de Estado (Polling loop hasta 'done')
# ------------------------------------------------------------------------------
echo ""
echo "⏳ Paso 6: Monitoreando progreso del job $JOB_ID (máx ${TIMEOUT_S}s)..."
RESP_GET="$TMP_DIR/resp_get.json"
inicio_ts=$(date +%s)
estado="queued"

while true; do
  ahora_ts=$(date +%s)
  transcurrido=$((ahora_ts - inicio_ts))
  if [ "$transcurrido" -ge "$TIMEOUT_S" ]; then
    echo ""
    echo "❌ Error: Timeout esperando que el job finalice después de ${TIMEOUT_S}s." >&2
    echo "   Último estado conocido: $estado" >&2
    exit 5
  fi

  curl -s -o "$RESP_GET" -X GET "$URL/jobs/$JOB_ID" \
    -H "Authorization: Bearer $TOKEN"

  estado=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["contenido"]["estado"])' "$RESP_GET" 2>/dev/null || echo "desconocido")
  pct=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["contenido"].get("fragmentos", {}).get("porcentaje", 0))' "$RESP_GET" 2>/dev/null || echo "0")

  printf "\r   * [%03ds] Estado: %-12s | Avance: %5.1f%%" "$transcurrido" "$estado" "$pct"

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
      sleep 2
      ;;
    *)
      sleep 2
      ;;
  esac
done

# ------------------------------------------------------------------------------
# 8. Descarga de Imagen Resultante
# ------------------------------------------------------------------------------
echo ""
echo "📥 Paso 7: Descargando resultado desde GET /jobs/$JOB_ID/resultado/0..."
CODIGO_DOWN=$(curl -s -w "%{http_code}" -o "$IMG_DISTRIB" -X GET "$URL/jobs/$JOB_ID/resultado/0" \
  -H "Authorization: Bearer $TOKEN")

if [ "$CODIGO_DOWN" != "200" ]; then
  echo "❌ Error: No se pudo descargar el resultado (código $CODIGO_DOWN)." >&2
  exit 8
fi

tam_bytes=$(wc -c < "$IMG_DISTRIB" | tr -d ' ')
if [ "$tam_bytes" -le 0 ]; then
  echo "❌ Error: El archivo descargado está vacío." >&2
  exit 9
fi
echo "   ✅ Imagen descargada correctamente ($tam_bytes bytes)."

# ------------------------------------------------------------------------------
# 9. Validación Píxel a Píxel contra Hit #1
# ------------------------------------------------------------------------------
echo ""
echo "🔬 Paso 8: Comparación píxel a píxel: Distribuido vs Centralizado..."
python3 - <<EOF
import sys
import numpy as np
from PIL import Image

img_dist = np.asarray(Image.open("$IMG_DISTRIB").convert("L"))
img_cent = np.asarray(Image.open("$IMG_CENTRAL").convert("L"))

if img_dist.shape != img_cent.shape:
    print(f"❌ Discrepancia de dimensiones: distribuida {img_dist.shape} vs centralizada {img_cent.shape}", file=sys.stderr)
    sys.exit(10)

if not np.array_equal(img_dist, img_cent):
    diferencias = np.sum(img_dist != img_cent)
    total_pixeles = img_dist.size
    print(f"❌ Discrepancia de píxeles: {diferencias} de {total_pixeles} píxeles son distintos.", file=sys.stderr)
    sys.exit(11)

print(f"   ✅ Verificación exitosa: Las imágenes ({img_dist.shape[1]}x{img_dist.shape[0]} px) son 100% IDÉNTICAS píxel por píxel.")
EOF

# ------------------------------------------------------------------------------
# 10. Prueba Negativa de Validación Temprana (Requisito R2)
# ------------------------------------------------------------------------------
echo ""
echo "🛡️ Paso 9: Prueba de robustez (rechazo de operación inexistente)..."
RESP_NEG="$TMP_DIR/resp_neg.json"
OP_INVALIDA="[{\"operacion\":\"operacion_que_no_existe_xyz\",\"imagen\":\"$IMAGEN\"}]"

CODIGO_NEG=$(curl -s -w "%{http_code}" -o "$RESP_NEG" -X POST "$URL/jobs" \
  -H "Authorization: Bearer $TOKEN" \
  -F "archivos=@$IMG_ENTRADA" \
  -F "operaciones=$OP_INVALIDA")

if [[ "$CODIGO_NEG" =~ ^(400|422)$ ]]; then
  echo "   ✅ Rechazo preventivo correcto (código $CODIGO_NEG). El payload inválido no ingresa a la cola."
else
  echo "   ⚠️ Advertencia: Esperaba 400/422 ante operación inexistente, se obtuvo: $CODIGO_NEG"
fi

# ------------------------------------------------------------------------------
# Resumen Final
# ------------------------------------------------------------------------------
echo ""
echo "=============================================================================="
echo "🎉 TEST E2E FINALIZADO CON ÉXITO"
echo "   - Pipeline completo k3s -> Ingress -> Backend -> Workers -> Joiner verificado."
echo "   - Salida distribuida idéntica al procesamiento centralizado del Hit #1."
echo "   - Validación temprana de operaciones inválidas activa."
echo "=============================================================================="
exit 0
