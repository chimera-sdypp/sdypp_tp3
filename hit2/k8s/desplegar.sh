#!/usr/bin/env bash
# Despliega el Hit #2 en k3s (namespace sobel-hit1): los workers del Hit #1, RabbitMQ, Redis y el master; y, si
# está el Hit #4, el ServiceMonitor del master y su dashboard de Grafana.
#
#   ./desplegar.sh
#
# Necesita la imagen sobel-hit2:local importada en k3s (../construir.sh) y, para que el master pueda crear VMs,
# infra/base aplicado con k3s_jwks (Workload Identity para este cluster). Sin eso el master queda sólo con los
# workers locales (SOBEL_MAX_VMS=0). Variables opcionales: SOBEL_LOCALES (2 workers locales), SOBEL_CPU_LOCAL
# (450m c/u: rinden como una VM), SOBEL_MAX_VMS (2), SOBEL_UMBRAL (3), SOBEL_MARGEN (1),
# SOBEL_ESTABILIZACION (30), TF_BUCKET y
# SOBEL_IMAGEN_MASTER (otra imagen del master, p. ej. de un registry local para probar sin importar).
set -euo pipefail
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$AQUI/../.."
NS=sobel-hit1
BASE=(terraform -chdir="$REPO/infra/base")

kubectl apply -f "$REPO/hit1/k8s/sobel.yaml"
kubectl scale deployment/sobel-worker -n "$NS" --replicas="${SOBEL_LOCALES:-2}"
# Un worker local limitado rinde como una VM e2-standard-2 (en CachyOS hay que apagar scx_lavd: ignora el límite).
kubectl set resources deployment/sobel-worker -n "$NS" --limits=cpu="${SOBEL_CPU_LOCAL:-450m}" \
  --requests=cpu="${SOBEL_CPU_LOCAL:-450m}"

# Usuario y clave de RabbitMQ: se generan la primera vez y después se reusan (nunca van al repo).
if ! kubectl get secret sobel-hit2-rabbitmq -n "$NS" >/dev/null 2>&1; then
  clave=$(head -c 48 /dev/urandom | base64 | tr -dc 'a-zA-Z0-9' | head -c 24)
  kubectl create secret generic sobel-hit2-rabbitmq -n "$NS" --from-literal=RABBITMQ_DEFAULT_USER=sobel \
    --from-literal=RABBITMQ_DEFAULT_PASS="$clave" --from-literal=RABBITMQ_URL="amqp://sobel:$clave@rabbitmq:5672/"
fi

# Lo que el master necesita de GCP, sacado de las salidas de infra/base (no son secretos).
proveedor=$("${BASE[@]}" output -raw proveedor_k3s 2>/dev/null || true)
maximo=${SOBEL_MAX_VMS:-2}
if [ -z "$proveedor" ] || [ "$proveedor" = null ]; then
  echo "infra/base no tiene Workload Identity para k3s: el master queda sólo con los workers locales" >&2
  proveedor="sin-workload-identity" maximo=0
fi
cuenta=$("${BASE[@]}" output -raw cuenta_ci)
registro=$("${BASE[@]}" output -raw registry)
proyecto=${registro#*/} proyecto=${proyecto%%/*}
credenciales=$(mktemp)
trap 'rm -f "$credenciales"' EXIT
cat > "$credenciales" <<JSON
{
  "type": "external_account",
  "audience": "//iam.googleapis.com/$proveedor",
  "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
  "token_url": "https://sts.googleapis.com/v1/token",
  "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/$cuenta:generateAccessToken",
  "credential_source": {"file": "/var/run/secrets/gcp/token", "format": {"type": "text"}}
}
JSON
kubectl create configmap sobel-master-gcp -n "$NS" --dry-run=client -o yaml \
  --from-literal=TF_VAR_proyecto="$proyecto" --from-literal=TF_VAR_imagen="$registro/sobel-hit1:1.1.0" \
  --from-literal=TF_BUCKET="${TF_BUCKET:-$proyecto-tfstate}" --from-literal=SOBEL_MAX_VMS="$maximo" \
  --from-literal=SOBEL_ESTABILIZACION="${SOBEL_ESTABILIZACION:-30}" --from-literal=SOBEL_UMBRAL="${SOBEL_UMBRAL:-3}" \
  --from-literal=SOBEL_MARGEN="${SOBEL_MARGEN:-1}" --from-file=credenciales.json="$credenciales" |
  kubectl apply -f -

kubectl apply -f "$AQUI/rabbitmq.yaml" -f "$AQUI/redis.yaml"
sed -e "s|__AUDIENCIA_WIF__|//iam.googleapis.com/$proveedor|" \
    -e "s|image: sobel-hit2:local|image: ${SOBEL_IMAGEN_MASTER:-sobel-hit2:local}|" "$AQUI/master.yaml" |
  kubectl apply -f -
# Métricas y dashboard, si el Hit #4 (kube-prometheus-stack, hit4/instalar_observabilidad.sh) está instalado.
if kubectl get crd servicemonitors.monitoring.coreos.com >/dev/null 2>&1; then
  kubectl apply -f "$AQUI/monitoreo.yaml"
  kubectl create configmap sobel-hit2-dashboard -n "$NS" --from-file=sobel-hit2.json="$AQUI/dashboard.json" \
    --dry-run=client -o yaml | kubectl label --local -f - grafana_dashboard=1 -o yaml | kubectl apply -f -
else
  echo "sin el Hit #4 (Prometheus y Grafana) instalado: el master expone /metrics, pero nadie lo lee" >&2
fi
kubectl rollout restart deployment/sobel-master -n "$NS" >/dev/null   # toma el ConfigMap nuevo
for deployment in rabbitmq redis sobel-master; do
  kubectl rollout status "deployment/$deployment" -n "$NS" --timeout=180s
done
echo "master en http://localhost:30080 (hasta $maximo VMs): curl localhost:30080/estado"
