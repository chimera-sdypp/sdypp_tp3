#!/usr/bin/env bash
# Hit #2 (cloud bursting): procesa una imagen con los workers locales del Hit #1 (en k3s) más N workers en la nube,
# y destruye las VMs al terminar, aunque algo falle.
#
#   ./desbordar.sh <N> entrada.jpg salida.png
#
# Necesita los workers del Hit #1 desplegados (kubectl apply -f ../hit1/k8s/sobel.yaml), hit2/infra/terraform.tfvars,
# terraform init hecho y credenciales de GCP (gcloud auth application-default login).
set -euo pipefail
N=$1 ENTRADA=$2 SALIDA=$3
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF=(terraform -chdir="$AQUI/infra")
source "$AQUI/master.sh"

kubectl rollout status deployment/sobel-worker -n sobel-hit1 --timeout=60s
trap 'echo "teardown: borrando las VMs"; "${TF[@]}" apply -auto-approve -input=false -var cantidad=0 >/dev/null' EXIT
inicio=$(date +%s)
"${TF[@]}" apply -auto-approve -input=false -var cantidad="$N"
nube=$("${TF[@]}" output -raw workers)

# Bootstrap: cada VM tarda en arrancar y bajar la imagen (alrededor de 1 minuto). Hasta 5 minutos.
for worker in ${nube//,/ }; do
  for _ in $(seq 100); do
    curl -sf "http://$worker/health" >/dev/null && break
    sleep 3
  done
  curl -sf "http://$worker/health" >/dev/null || { echo "$worker no arrancó" >&2; exit 1; }
done
echo "VMs listas en $(($(date +%s) - inicio)) s: $nube"

master "sobel-worker:8080,$nube" < "$ENTRADA" > "$SALIDA"
echo "imagen procesada en $(($(date +%s) - inicio)) s en total"
