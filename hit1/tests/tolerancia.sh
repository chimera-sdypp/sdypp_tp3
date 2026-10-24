#!/usr/bin/env bash
# Etapa 3 con Docker real: workers que se caen o no responden en medio del trabajo y el
# resultado igual tiene que ser idéntico a la Etapa 1. Uso: bash hit1/tests/tolerancia.sh
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python3}
tmp=$(mktemp -d)
trap 'docker compose --profile fallas down -v >/dev/null 2>&1; rm -rf "$tmp"' EXIT

"$PYTHON" -c "
import numpy as np; from PIL import Image
Image.fromarray(np.random.default_rng(0).integers(0, 256, (300, 200, 3), dtype=np.uint8)).save('$tmp/entrada.png')"
SOBEL_LOGS="$tmp/logs" "$PYTHON" centralizado.py "$tmp/entrada.png" "$tmp/central.png"

igual_a_centralizado() {
  "$PYTHON" -c "
import numpy as np, sys; from PIL import Image
a, b = (np.asarray(Image.open(f'$tmp/{n}.png')) for n in ('$1', 'central'))
sys.exit(not np.array_equal(a, b))"
}

docker compose build -q worker

echo "== Escenario 1: un worker que se cae y otro que no responde (SOBEL_FALLA) =="
docker compose --profile fallas up -d --wait --scale worker=2
SOBEL_PARTES=8 SOBEL_TIMEOUT=3 docker compose run --rm -T master \
  < "$tmp/entrada.png" > "$tmp/fallas.png" 2> "$tmp/fallas.log"
grep -v Container "$tmp/fallas.log"
igual_a_centralizado fallas
grep -q "no está disponible" "$tmp/fallas.log"   # el caído cortó la conexión
grep -q "no respondió en 3 s" "$tmp/fallas.log"  # el colgado venció el timeout
[ "$(docker inspect -f '{{.State.Status}}' "$(docker compose ps -aq worker-caido)")" = exited ]
echo "OK: salida idéntica a la centralizada con un worker caído y otro colgado"
docker compose --profile fallas down >/dev/null 2>&1

echo "== Escenario 2: docker kill de un worker sano mientras procesa =="
SOBEL_DEMORA=4 docker compose up -d --wait --scale worker=3 worker
SOBEL_PARTES=6 SOBEL_TIMEOUT=6 docker compose run --rm -T master \
  < "$tmp/entrada.png" > "$tmp/kill.png" 2> "$tmp/kill.log" &
master=$!
# Se mata un worker recién cuando el master ya repartió (cada franja tarda 4 s por la demora).
for _ in $(seq 100); do grep -q "franjas entre" "$tmp/kill.log" 2>/dev/null && break; sleep 0.1; done
sleep 1
docker kill "$(docker compose ps -q worker | head -1)" >/dev/null
wait "$master"
grep -v Container "$tmp/kill.log"
igual_a_centralizado kill
grep -q "reasignaciones" "$tmp/kill.log"
echo "OK: salida idéntica a la centralizada tras matar un worker en medio del trabajo"

echo "OK: tolerancia a fallos"
