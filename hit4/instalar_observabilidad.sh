#!/usr/bin/env bash
# ==============================================================================
# Despliegue de Observabilidad — Hit #4 (Prometheus, Grafana y Alertas)
# ==============================================================================
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
NAMESPACE_APP="sobel"
NAMESPACE_MONITORING="monitoring"

echo "=========================================================="
echo "  Hit #4: Despliegue de Observabilidad con Helm y K8s"
echo "=========================================================="

# 1. Verificar herramientas
command -v kubectl >/dev/null 2>&1 || { echo "❌ kubectl no está instalado."; exit 1; }
command -v helm >/dev/null 2>&1 || { echo "❌ helm no está instalado. Instale Helm antes de continuar."; exit 1; }

# 2. Agregar repo de Helm
echo "[1/5] Actualizando repositorios de Helm..."
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
helm repo update prometheus-community

# 3. Desplegar kube-prometheus-stack en el grupo infra
echo "[2/5] Desplegando kube-prometheus-stack en namespace '${NAMESPACE_MONITORING}' (pool=infra)..."
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
  --namespace "${NAMESPACE_MONITORING}" \
  --create-namespace \
  -f "${DIR}/values.yaml"

# 4. Desplegar ServiceMonitors y PrometheusRules en namespace sobel
echo "[3/5] Aplicando ServiceMonitors y reglas de alerta en namespace '${NAMESPACE_APP}'..."
kubectl apply -f "${DIR}/servicemonitor-rabbitmq.yaml"
kubectl apply -f "${DIR}/servicemonitor-sobel.yaml"
kubectl apply -f "${DIR}/alertrules.yaml"

# 5. Cargar Dashboard de Grafana vía ConfigMap para autodetección por Sidecar
echo "[4/5] Aprovisionando Dashboard de Grafana en '${NAMESPACE_APP}'..."
kubectl create configmap sobel-grafana-dashboard \
  --from-file=sobel-dashboard.json="${DIR}/dashboards/sobel-dashboard.json" \
  --namespace="${NAMESPACE_APP}" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl label configmap sobel-grafana-dashboard grafana_dashboard=1 --namespace="${NAMESPACE_APP}" --overwrite

echo ""
echo "[5/5] ¡Observabilidad desplegada con éxito!"
echo "=========================================================="
echo "  Instrucciones de acceso:"
echo "  - Grafana (admin / admin):"
echo "      kubectl port-forward svc/prometheus-grafana 3000:80 -n ${NAMESPACE_MONITORING}"
echo "      Navegador: http://localhost:3000"
echo "  - Prometheus Server:"
echo "      kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090 -n ${NAMESPACE_MONITORING}"
echo "      Navegador: http://localhost:9090"
echo "=========================================================="
