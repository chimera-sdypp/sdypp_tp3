#!/usr/bin/env bash
# Hit #4 en el GKE compartido: el cluster ya tiene kube-prometheus-stack y Loki en el namespace monitoring (son del
# otro grupo). Acá sólo se agregan nuestros ServiceMonitors, las alertas y el dashboard, en nuestro namespace.
set -euo pipefail
NS="${SOBEL_NAMESPACE:-chimera-sobel}"
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
for f in servicemonitor-rabbitmq.yaml servicemonitor-sobel.yaml alertrules.yaml; do
  sed -E "s/^(\s+namespace:) sobel$/\1 $NS/; s/^(\s+- )sobel$/\1$NS/; s/namespace=\"sobel\"/namespace=\"$NS\"/g" \
    "$RAIZ/hit4/$f" | kubectl apply -f -
done
# El sidecar de Grafana carga los ConfigMaps con grafana_dashboard=1 de todos los namespaces.
python3 - "$RAIZ/hit4/dashboards/sobel-dashboard.json" "$NS" > /tmp/dashboard-$NS.json <<'PY'
import json, re, sys
d = json.load(open(sys.argv[1])); ns = sys.argv[2]
d["uid"], d["title"] = f"{ns}-hit4", f"Chimera — Sobel distribuido (Hit #4, namespace {ns})"
print(re.sub(r'namespace(\\?)=(~?)(\\?)"sobel', rf'namespace\1=\2\3"{ns}', json.dumps(d, ensure_ascii=False)))
PY
kubectl create configmap "$NS-dashboard" -n "$NS" --from-file="$NS.json=/tmp/dashboard-$NS.json" --dry-run=client -o yaml \
  | kubectl label --local -f - grafana_dashboard=1 -o yaml | kubectl apply -f -
