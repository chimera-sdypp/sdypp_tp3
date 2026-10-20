#!/usr/bin/env bash
# ==============================================================================
# Generación de Kubernetes Secrets para Hit #3 (Sobel Asincrónico)
# ==============================================================================
# Genera secretos criptográficamente seguros para RabbitMQ, Redis y firma JWT.
# No utiliza contraseñas por defecto predecibles en repositorios públicos.
# Guarda una copia local en .env.secrets (ignorado en .gitignore) con permisos 600.
# ==============================================================================
set -euo pipefail

NAMESPACE="${SOBEL_NAMESPACE:-sobel}"
DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR_RAIZ="$(cd "$DIR_SCRIPT/../.." && pwd)"
ARCHIVO_ENV_SECRETS="$DIR_RAIZ/.env.secrets"

generar_clave() {
  local longitud="${1:-24}"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -base64 48 | tr -dc 'a-zA-Z0-9' | head -c "$longitud"
  else
    head -c 64 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c "$longitud"
  fi
}

echo "==> Verificando existencia del namespace '${NAMESPACE}'..."
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || kubectl apply -f "$DIR_SCRIPT/00-namespace.yaml"

# 1. RabbitMQ: Usuario y contraseña seguros
RABBITMQ_USER="${RABBITMQ_USER:-sobel_admin}"
RABBITMQ_PASS="${RABBITMQ_PASS:-$(generar_clave 24)}"
RABBITMQ_HOST="${RABBITMQ_HOST:-rabbitmq.${NAMESPACE}.svc.cluster.local}"
RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
RABBITMQ_URL="amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}:${RABBITMQ_PORT}/"

# 2. Redis: Contraseña segura requerida
REDIS_PASS="${REDIS_PASS:-$(generar_clave 24)}"
REDIS_HOST="${REDIS_HOST:-redis.${NAMESPACE}.svc.cluster.local}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_URL="redis://:${REDIS_PASS}@${REDIS_HOST}:${REDIS_PORT}/0"

# 3. JWT HS256: Secreto de alta entropía para firma de tokens
SOBEL_SECRETO="${SOBEL_SECRETO:-$(generar_clave 32)}"

echo "==> Creando/actualizando Kubernetes Secret 'sobel-secretos' en namespace '${NAMESPACE}'..."
kubectl create secret generic sobel-secretos \
  --namespace="${NAMESPACE}" \
  --from-literal=RABBITMQ_DEFAULT_USER="${RABBITMQ_USER}" \
  --from-literal=RABBITMQ_DEFAULT_PASS="${RABBITMQ_PASS}" \
  --from-literal=RABBITMQ_USER="${RABBITMQ_USER}" \
  --from-literal=RABBITMQ_PASS="${RABBITMQ_PASS}" \
  --from-literal=RABBITMQ_URL="${RABBITMQ_URL}" \
  --from-literal=REDIS_PASS="${REDIS_PASS}" \
  --from-literal=REDIS_URL="${REDIS_URL}" \
  --from-literal=SOBEL_SECRETO="${SOBEL_SECRETO}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "✅ Secreto 'sobel-secretos' configurado correctamente con credenciales dinámicas seguras."

# Guardar copia local en .env.secrets (restringido con chmod 600, ignorado en git)
cat <<EOF > "$ARCHIVO_ENV_SECRETS"
# Credenciales generadas para el cluster Kubernetes (Hit #3)
# Archivo local sensible: NO COMMITEAR (ignorado en .gitignore)
RABBITMQ_USER=${RABBITMQ_USER}
RABBITMQ_PASS=${RABBITMQ_PASS}
RABBITMQ_URL=${RABBITMQ_URL}
REDIS_PASS=${REDIS_PASS}
REDIS_URL=${REDIS_URL}
SOBEL_SECRETO=${SOBEL_SECRETO}
EOF
chmod 600 "$ARCHIVO_ENV_SECRETS"
echo "ℹ️  Copia local de credenciales guardada en '${ARCHIVO_ENV_SECRETS}' (permisos 600)."

# Configuración opcional de imagePullSecret para GHCR si se definen las variables
if [[ -n "${GHCR_USER:-}" && -n "${GHCR_PAT:-}" ]]; then
  echo "==> Configurando imagePullSecret 'ghcr-secret' para imágenes privadas..."
  kubectl create secret docker-registry ghcr-secret \
    --namespace="${NAMESPACE}" \
    --docker-server="ghcr.io" \
    --docker-username="${GHCR_USER}" \
    --docker-password="${GHCR_PAT}" \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "✅ Secreto 'ghcr-secret' configurado."
else
  echo "ℹ️  No se definieron GHCR_USER ni GHCR_PAT. Se omitió la creación de 'ghcr-secret'."
fi
