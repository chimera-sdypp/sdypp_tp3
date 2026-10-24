#!/usr/bin/env bash
# R1: las imágenes de GHCR son privadas. El mismo pod sin imagePullSecret queda en ImagePullBackOff
# y con el secret pasa a Running.
#
#   GHCR_USER=<usuario> GHCR_PAT=<token con read:packages> ./r1.sh [imagen]
#
# El token sólo lee paquetes y vive en el Secret del cluster, nunca en el repo. En la nube no hace
# falta: el nodo baja la imagen con su identidad (Workload Identity / rol de IAM).
set -euo pipefail

IMAGEN="${1:-ghcr.io/chimera-sdypp/sobel-backend:dev}"
NS=sobel-r1
: "${GHCR_USER:?falta GHCR_USER}" "${GHCR_PAT:?falta GHCR_PAT}"

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create secret docker-registry ghcr-secret --docker-server=ghcr.io \
  --docker-username="$GHCR_USER" --docker-password="$GHCR_PAT" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" delete pod sin-secret con-secret --ignore-not-found --wait

# imagePullPolicy Always: si no, el pod sin secret arrancaría con la imagen que dejó en caché el otro.
# sleep en vez del servicio: lo que se prueba es la descarga, no que el backend tenga RabbitMQ.
for nombre in sin-secret con-secret; do
  secretos="[]"
  [ "$nombre" = con-secret ] && secretos='[{"name": "ghcr-secret"}]'
  kubectl -n "$NS" apply -f - <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $nombre
spec:
  imagePullSecrets: $secretos
  containers:
    - name: app
      image: $IMAGEN
      imagePullPolicy: Always
      command: ["sleep", "infinity"]
YAML
done

kubectl -n "$NS" wait pod/con-secret --for=condition=Ready --timeout=180s
for _ in $(seq 60); do
  motivo=$(kubectl -n "$NS" get pod sin-secret -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}')
  [ "$motivo" = ImagePullBackOff ] && break
  sleep 2
done

kubectl -n "$NS" get pods -o wide
echo
kubectl -n "$NS" get events --field-selector involvedObject.name=sin-secret,reason=Failed \
  -o custom-columns=MOTIVO:.reason,MENSAJE:.message | head -3
echo
echo "Para borrar la prueba: kubectl delete namespace $NS"
