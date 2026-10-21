#!/usr/bin/env bash
# Hit #2 (cloud bursting): procesa una imagen con los workers locales del Hit #1 más N workers en la nube,
# y destruye las VMs al terminar, aunque algo falle.
#
#   ./desbordar.sh <N> entrada.jpg salida.png
#
# Necesita hit2/infra/terraform.tfvars, terraform init hecho y credenciales de GCP
# (gcloud auth application-default login).
set -euo pipefail
N=$1 ENTRADA=$2 SALIDA=$3
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF=(terraform -chdir="$AQUI/infra")
COMPOSE=(docker compose -f "$AQUI/../hit1/docker-compose.yml")

trap 'echo "teardown: borrando las VMs"; "${TF[@]}" apply -auto-approve -input=false -var cantidad=0 >/dev/null' EXIT
inicio=$(date +%s)
"${TF[@]}" apply -auto-approve -input=false -var cantidad="$N"
nube=$("${TF[@]}" output -raw workers)

# Bootstrap: cada VM tarda en instalar Docker y bajar la imagen. Hasta 5 minutos.
for worker in ${nube//,/ }; do
  for _ in $(seq 100); do
    curl -sf "http://$worker/health" >/dev/null && break
    sleep 3
  done
  curl -sf "http://$worker/health" >/dev/null || { echo "$worker no arrancó" >&2; exit 1; }
done
echo "VMs listas en $(($(date +%s) - inicio)) s: $nube"

"${COMPOSE[@]}" up -d --build --wait worker
"${COMPOSE[@]}" run --rm -T -e SOBEL_WORKERS="worker:8080,$nube" master < "$ENTRADA" > "$SALIDA"
echo "imagen procesada en $(($(date +%s) - inicio)) s en total"
