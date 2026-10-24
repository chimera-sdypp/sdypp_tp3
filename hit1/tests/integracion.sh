#!/usr/bin/env bash
# Etapa 2 con Docker real: 3 workers, el master por compose y el resultado comparado con
# la Etapa 1. Uso (desde cualquier lado): bash hit1/tests/integracion.sh
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python3}
tmp=$(mktemp -d)
trap 'docker compose down -v >/dev/null 2>&1; rm -rf "$tmp"' EXIT

docker compose up -d --build --wait --scale worker=3 worker

"$PYTHON" -c "
import numpy as np; from PIL import Image
Image.fromarray(np.random.default_rng(0).integers(0, 256, (300, 200, 3), dtype=np.uint8)).save('$tmp/entrada.png')"

SOBEL_PARTES=6 docker compose run --rm -T master < "$tmp/entrada.png" > "$tmp/distribuida.png"
SOBEL_LOGS="$tmp/logs" "$PYTHON" centralizado.py "$tmp/entrada.png" "$tmp/central.png"
# Se comparan los píxeles: los bytes del PNG cambian con la versión de zlib.
"$PYTHON" -c "
import numpy as np, sys; from PIL import Image
a, b = (np.asarray(Image.open(f'$tmp/{n}.png')) for n in ('distribuida', 'central'))
sys.exit(not np.array_equal(a, b))"
echo "OK: la salida distribuida es idéntica a la centralizada"

# Cada worker tiene que haber procesado 2 de las 6 franjas.
for id in $(docker compose ps -q worker); do
  n=$(docker logs "$id" 2>&1 | grep -c "procesada" || true)
  echo "$(docker inspect -f '{{.Name}}' "$id"): $n franjas"
  [ "$n" -eq 2 ]
done

# Una imagen inválida termina con error claro y código 2.
codigo=0
echo "no soy una imagen" | docker compose run --rm -T master > /dev/null 2> "$tmp/error.log" || codigo=$?
cat "$tmp/error.log"
[ "$codigo" -eq 2 ] && grep -q "no se pudo procesar" "$tmp/error.log"
echo "OK: integración"
