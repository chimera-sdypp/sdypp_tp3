#!/usr/bin/env bash
# Construye la imagen del master del Hit #2 (sobel-hit2:local) con un contexto chico: el reparto del Hit #1, el
# servicio, el escalador y el almacén en disco, y el Terraform de las VMs (sin terraform.tfvars ni .terraform/).
#   ./construir.sh && docker save sobel-hit2:local | sudo k3s ctr images import -
set -euo pipefail
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
contexto=$(mktemp -d)
trap 'rm -rf "$contexto"' EXIT
cp "$AQUI"/{Dockerfile,requirements.txt,servicio.py,escalador.py,almacen.py} "$AQUI"/../hit1/{master.py,sobel.py,registro.py} \
  "$contexto/"
mkdir "$contexto/infra"
cp "$AQUI"/infra/{*.tf,*.tftpl,.terraform.lock.hcl} "$contexto/infra/"
docker build -t "${1:-sobel-hit2:local}" "$contexto"
