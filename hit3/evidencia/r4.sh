#!/usr/bin/env bash
# R4: la identidad viaja en cada pedido, los roles se respetan y el backend no guarda estado.
# Escala el backend a 3 réplicas, prueba cada regla contra el ingress y muestra qué réplicas atendieron.
#
#   ./r4.sh [url del ingress]       (default http://localhost, el traefik del k3s)
#
# Los tokens se emiten adentro de un pod del backend: el SOBEL_SECRETO no sale del cluster.
# Necesita al menos un worker de $SOBEL_IMAGEN activo para poder crear un job.
set -euo pipefail

URL="${1:-http://localhost}"
NS="${SOBEL_NAMESPACE:-sobel}"
IMAGEN="${SOBEL_IMAGEN:-ghcr.io/chimera-sdypp/sobel-worker:1.0.0}"
PEDIDOS=30
CUERPO=$(mktemp)
PNG=$(mktemp --suffix=.png)
trap 'rm -f "$CUERPO" "$PNG"' EXIT
echo "iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAAAGklEQVR42mM8oaHBQApgYiARjGoY1TB0NAAAjC0BOJDndDkAAAAASUVORK5CYII=" \
  | base64 -d > "$PNG"
fallas=0

token() {
  kubectl -n "$NS" exec deploy/backend -- python -m backend.emitir_token "$1" "$2" --horas 1
}

# pedir METODO RUTA TOKEN [más argumentos de curl]: imprime el código y deja el cuerpo en $CUERPO
pedir() {
  local metodo=$1 ruta=$2 tok=$3
  shift 3
  curl -s -o "$CUERPO" -w '%{http_code}' -X "$metodo" ${tok:+-H "Authorization: Bearer $tok"} "$@" "$URL$ruta"
}

esperar() {
  local que=$1 esperado=$2 obtenido=$3
  if [ "$esperado" = "$obtenido" ]; then
    echo "  ok   $que → $obtenido"
  else
    echo "  MAL  $que → $obtenido (esperaba $esperado): $(head -c 200 "$CUERPO")"
    fallas=$((fallas + 1))
  fi
}

echo "== Backend con 3 réplicas"
kubectl -n "$NS" scale deploy/backend --replicas=3
kubectl -n "$NS" rollout status deploy/backend --timeout=180s
kubectl -n "$NS" get pods -l app.kubernetes.io/name=backend -o wide

ana=$(token ana usuario)
beto=$(token beto usuario)
admin=$(token root admin)

echo "== Sin identidad"
esperar "GET /health (pública)" 200 "$(pedir GET /health "")"
esperar "GET /jobs sin token" 401 "$(pedir GET /jobs "")"
esperar "GET /jobs con un token mal firmado" 401 "$(pedir GET /jobs "${ana%.*}.firmaFalsa")"

echo "== Roles"
esperar "usuario en GET /stats" 403 "$(pedir GET /stats "$ana")"
esperar "usuario en POST /admin/dlq/reprocesar" 403 "$(pedir POST /admin/dlq/reprocesar "$ana")"
esperar "admin en GET /stats" 200 "$(pedir GET /stats "$admin")"

echo "== Jobs de otro usuario"
creado=$(pedir POST /jobs "$ana" -F "archivos=@$PNG" -F "operaciones=[{\"operacion\":\"sobel\",\"imagen\":\"$IMAGEN\"}]")
esperar "ana crea un job" 202 "$creado"
# Sin job no hay nada que consultar: con SIN_WORKERS falta desplegar un worker de $IMAGEN.
[ "$creado" = 202 ] || { echo "R4: no se pudo crear el job, corto acá"; exit 1; }
job=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["contenido"]["job_id"])' "$CUERPO")
esperar "ana ve su job" 200 "$(pedir GET "/jobs/$job" "$ana")"
esperar "beto pide el job de ana" 404 "$(pedir GET "/jobs/$job" "$beto")"
esperar "beto pide el resultado de ana" 404 "$(pedir GET "/jobs/$job/resultado/0" "$beto")"
esperar "admin ve el job de ana" 200 "$(pedir GET "/jobs/$job" "$admin")"

echo "== Sin estado: $PEDIDOS consultas al mismo job, las reparte el Service entre las réplicas"
for _ in $(seq "$PEDIDOS"); do
  codigo=$(pedir GET "/jobs/$job" "$ana")
  [ "$codigo" = 200 ] || { esperar "GET /jobs/$job" 200 "$codigo"; break; }
done
atendieron=0
for pod in $(kubectl -n "$NS" get pods -l app.kubernetes.io/name=backend -o name); do
  n=$(kubectl -n "$NS" logs "$pod" --since=10m | grep -c "GET /jobs/$job " || true)
  echo "  ${pod#pod/}: $n pedidos"
  if [ "$n" -gt 0 ]; then atendieron=$((atendieron + 1)); fi
done
: > "$CUERPO"  # este chequeo no viene de una respuesta HTTP
esperar "réplicas que atendieron" 3 "$atendieron"

echo
if [ "$fallas" -eq 0 ]; then echo "R4: todo ok"; else echo "R4: $fallas fallas"; fi
exit "$fallas"
