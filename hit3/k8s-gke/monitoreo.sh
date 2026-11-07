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

def filtrar(expr):
    # En un cluster compartido, toda métrica de RabbitMQ o de nuestros servicios se filtra por nuestro namespace.
    expr = re.sub(r'namespace=(~?)"sobel"', rf'namespace=\1"{ns}"', expr)
    def con_ns(m):
        nombre, etiquetas = m.group(1), m.group(2)
        if etiquetas is None:
            return f'{nombre}{{namespace="{ns}"}}'
        return m.group(0) if "namespace" in etiquetas else f'{nombre}{{namespace="{ns}",{etiquetas[1:]}'
    return re.sub(r'\b((?:rabbitmq|sobel)_[a-z_]+)(\{[^}]*\})?', con_ns, expr)

def recorrer(nodo):
    if isinstance(nodo, dict):
        for k, v in nodo.items():
            nodo[k] = filtrar(v) if k in ("expr", "query") and isinstance(v, str) else recorrer(v)
    elif isinstance(nodo, list):
        return [recorrer(x) for x in nodo]
    return nodo

print(json.dumps(recorrer(d), ensure_ascii=False))
PY
kubectl create configmap "$NS-dashboard" -n "$NS" --from-file="$NS.json=/tmp/dashboard-$NS.json" --dry-run=client -o yaml \
  | kubectl label --local -f - grafana_dashboard=1 -o yaml | kubectl apply -f -
