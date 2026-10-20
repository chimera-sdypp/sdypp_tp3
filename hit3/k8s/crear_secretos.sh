#!/usr/bin/env bash
# ==============================================================================
# Generación de Kubernetes Secrets para Hit #3 (Sobel Asincrónico)
# ==============================================================================
# Este script crea los secretos necesarios en el namespace 'sobel' sin dejar
# contraseñas ni tokens hardcodeados en el repositorio Git (cumpliendo R1 y gitleaks).
# ==============================================================================
set -euo pipefail

NAMESPACE="${SOBEL_NAMESPACE:-sobel}"

echo "==> Verificando existencia del namespace '${NAMESPACE}'..."
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || kubectl apply -f "$(dirname "$0")/00-namespace.yaml"

# Valores por defecto para desarrollo local en k3s
RABBITMQ_USER="${RABBITMQ_USER:-guest}"
RABBITMQ_PASS="${RABBITMQ_PASS:-guest}"
RABBITMQ_HOST="${RABBITMQ_HOST:-rabbitmq.${NAMESPACE}.svc.cluster.local}"
RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
RABBITMQ_URL="amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}:${RABBITMQ_PORT}/"

REDIS_HOST="${REDIS_HOST:-redis.${NAMESPACE}.svc.cluster.local}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_URL="redis://${REDIS_HOST}:${REDIS_PORT}/0"

# Secreto para firma y verificación de tokens JWT HS256
SOBEL_SECRETO="${SOBEL_SECRETO:-sobel-clave-secreta-jwt-hs256-hit3-distribuido}"

echo "==> Creando/actualizando Kubernetes Secret 'sobel-secretos' en namespace '${NAMESPACE}'..."
kubectl create secret generic sobel-secretos \
  --namespace="${NAMESPACE}" \
  --from-literal=RABBITMQ_DEFAULT_USER="${RABBITMQ_USER}" \
  --from-literal=RABBITMQ_DEFAULT_PASS="${RABBITMQ_PASS}" \
  --from-literal=RABBITMQ_URL="${RABBITMQ_URL}" \
  --from-literal=REDIS_URL="${REDIS_URL}" \
  --from-literal=SOBEL_SECRETO="${SOBEL_SECRETO}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "✅ Secreto 'sobel-secretos' configurado correctamente."

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
  echo "ℹ️  No se definieron GHCR_USER ni GHCR_PAT. Se omitió la creación de 'ghcr-secret' (usando imágenes locales si aplica)."
fi
