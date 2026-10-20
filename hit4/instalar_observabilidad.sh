#!/usr/bin/env bash
# ==============================================================================
# Despliegue de Observabilidad — Hit #4 (Prometheus, Grafana y Alertas)
# ==============================================================================
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
DIR_RAIZ="$(cd "${DIR}/.." && pwd)"
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

# 3. Asegurar credenciales dinámicas de Grafana (sin contraseñas fijas)
echo "[2/5] Asegurando credenciales seguras de Grafana..."
kubectl create namespace "${NAMESPACE_MONITORING}" --dry-run=client -o yaml | kubectl apply -f -
if ! kubectl get secret grafana-admin-credentials -n "${NAMESPACE_MONITORING}" >/dev/null 2>&1; then
  GRAFANA_PASS="${GRAFANA_ADMIN_PASSWORD:-$(openssl rand -base64 16 | tr -dc 'a-zA-Z0-9' | head -c 16)}"
  kubectl create secret generic grafana-admin-credentials \
    --namespace="${NAMESPACE_MONITORING}" \
    --from-literal=admin-user=admin \
    --from-literal=admin-password="${GRAFANA_PASS}"
  echo "    Contraseña generada para Grafana (usuario 'admin'): ${GRAFANA_PASS}"
  if [ -f "${DIR_RAIZ}/.env.secrets" ]; then
    echo "GRAFANA_ADMIN_USER=admin" >> "${DIR_RAIZ}/.env.secrets"
    echo "GRAFANA_ADMIN_PASS=${GRAFANA_PASS}" >> "${DIR_RAIZ}/.env.secrets"
  fi
else
  GRAFANA_PASS="$(kubectl get secret grafana-admin-credentials -n "${NAMESPACE_MONITORING}" -o jsonpath='{.data.admin-password}' | base64 -d)"
  echo "    Secreto 'grafana-admin-credentials' existente reutilizado."
fi

# 4. Desplegar kube-prometheus-stack en el grupo infra
echo "[3/5] Desplegando kube-prometheus-stack en namespace '${NAMESPACE_MONITORING}' (sobel/infra=true)..."
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
  --namespace "${NAMESPACE_MONITORING}" \
  --create-namespace \
  -f "${DIR}/values.yaml"

# 5. Desplegar ServiceMonitors y PrometheusRules en namespace sobel
echo "[4/5] Aplicando ServiceMonitors y reglas de alerta en namespace '${NAMESPACE_APP}'..."
kubectl apply -f "${DIR}/servicemonitor-rabbitmq.yaml"
kubectl apply -f "${DIR}/servicemonitor-sobel.yaml"
kubectl apply -f "${DIR}/alertrules.yaml"

# 6. Cargar Dashboard de Grafana vía ConfigMap para autodetección por Sidecar
echo "[5/5] Aprovisionando Dashboard de Grafana en '${NAMESPACE_APP}'..."
kubectl create configmap sobel-grafana-dashboard \
  --from-file=sobel-dashboard.json="${DIR}/dashboards/sobel-dashboard.json" \
  --namespace="${NAMESPACE_APP}" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl label configmap sobel-grafana-dashboard grafana_dashboard=1 --namespace="${NAMESPACE_APP}" --overwrite

echo ""
echo "¡Observabilidad desplegada con éxito!"
echo "=========================================================="
echo "  Instrucciones de acceso:"
echo "  - Grafana (usuario: 'admin' | clave: '${GRAFANA_PASS}'):"
echo "      kubectl port-forward svc/prometheus-grafana 3000:80 -n ${NAMESPACE_MONITORING}"
echo "      Navegador: http://localhost:3000"
echo "  - Prometheus Server:"
echo "      kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090 -n ${NAMESPACE_MONITORING}"
echo "      Navegador: http://localhost:9090"
echo "=========================================================="
