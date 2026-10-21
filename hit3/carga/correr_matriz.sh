#!/usr/bin/env bash
# ==============================================================================
# correr_matriz.sh — Runner automatizado de pruebas de carga k6 para Hit #3 (R5)
# Sistemas Distribuidos y Paralelos — Grupo Cerberus (Salva)
# ==============================================================================
set -euo pipefail

DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR_DATOS="$DIR_SCRIPT/datos"
DIR_RESULTADOS="$DIR_SCRIPT/resultados"
mkdir -p "$DIR_RESULTADOS"

TARGET_URL="${TARGET_URL:-http://localhost}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-5s}"
WORKER_COUNT="${V3:-2}"

# 1. Verificar o generar datos sintéticos
asegurar_datos() {
  if [ ! -f "$DIR_DATOS/tokens.json" ] || [ ! -f "$DIR_DATOS/img_1mb.png" ] || [ ! -f "$DIR_DATOS/img_50mb.png" ]; then
    echo "--- [1/3] Generando datos de prueba sintéticos ---"
    python3 "$DIR_SCRIPT/generar_datos.py"
  else
    echo "--- [1/3] Datos de prueba verificados en: $DIR_DATOS ---"
  fi
}

# 2. Determinar ejecutor de k6 (binario local o Docker)
determinar_k6() {
  if command -v k6 &> /dev/null; then
    K6_CMD="k6 run"
    echo "[INFO] Usando binario k6 local: $(which k6)"
  elif command -v docker &> /dev/null; then
    K6_CMD="docker run --rm -i --network=host -v $DIR_SCRIPT:/app -w /app grafana/k6:latest run"
    echo "[INFO] Usando k6 mediante contenedor Docker (grafana/k6)"
  else
    echo "[ERROR] No se encontró ni 'k6' ni 'docker'. Debe instalar k6 o Docker para correr las pruebas."
    exit 1
  fi
}

# 3. Función para ejecutar una celda
correr_celda() {
  local va="$1"
  local vb="$2"
  local vc="$3"
  local v1="$4"
  local dur="${5:-$DURATION}"
  local timestamp
  timestamp="$(date +%Y%m%d_%H%M%S)"
  local salida_json="$DIR_RESULTADOS/celda_va${va}_vb${vb}_vc${vc}_v1${v1}_${timestamp}.json"

  echo "========================================================================"
  echo " Ejecutando Celda: VA=$va archivos | VB=$vb ops | VC=$vc VUs | V1=$v1 | V3=$WORKER_COUNT"
  echo " Duración: $dur (Ramp-up: $RAMP_UP) | Destino: $TARGET_URL"
  echo "========================================================================"

  # Variables de entorno pasadas a k6
  TARGET_URL="$TARGET_URL" \
  VA="$va" \
  VB="$vb" \
  VC="$vc" \
  V1="$v1" \
  V3="$WORKER_COUNT" \
  DURATION="$dur" \
  RAMP_UP="$RAMP_UP" \
  $K6_CMD \
    --env TARGET_URL="$TARGET_URL" \
    --env VA="$va" \
    --env VB="$vb" \
    --env VC="$vc" \
    --env V1="$v1" \
    --env V3="$WORKER_COUNT" \
    --env DURATION="$dur" \
    --env RAMP_UP="$RAMP_UP" \
    --summary-export="$salida_json" \
    matriz_r5.js || true

  echo "[OK] Resultados de celda guardados en: $salida_json"
}

# 4. Modo Smoke Test
correr_smoke() {
  asegurar_datos
  determinar_k6
  echo "========================================================================"
  echo " Ejecutando Smoke Test contra: $TARGET_URL"
  echo "========================================================================"
  $K6_CMD --env TARGET_URL="$TARGET_URL" smoke_test.js
}

# 5. Generar informe Markdown consolidado a partir de los JSONs
generar_informe_md() {
  local timestamp
  timestamp="$(date +%Y%m%d_%H%M%S)"
  local informe_md="$DIR_RESULTADOS/informe_matriz_${timestamp}.md"

  cat <<EOF > "$informe_md"
# Informe de Pruebas de Carga (Matriz R5) — Hit #3
**Fecha:** $(date '+%Y-%m-%d %H:%M:%S')  
**Destino evaluado:** \`$TARGET_URL\`  
**Workers activos (V3):** $WORKER_COUNT  

## 1. Criterios de Evaluación del SLO (Sección 8 del Contrato)
Una celda **cumple** si satisface las tres condiciones simultáneamente:
1. **SLO 1:** $p_{95}$ de \`POST /jobs\` $\le 1000\text{ ms}$.
2. **SLO 2:** $p_{95}$ de \`tiempos_s.total\` $\le 30\text{ s}$ para pedidos de hasta 10 MB, y $\le 120\text{ s}$ para pedidos mayores.
3. **SLO 3:** Tasa de errores no provocados $< 1\%$.

## 2. Resumen de Resultados por Celda

| Celda (VA × VB × VC × V1) | p95 POST (ms) | p95 Total (s) | Error (%) | SLO 1 (POST <= 1s) | SLO 2 (Total <= 30s) | SLO 3 (Err < 1%) | Estado General |
|---|---|---|---|---|---|---|---|
EOF

  # Procesar cada JSON generado en la sesión
  for archivo in "$DIR_RESULTADOS"/celda_*.json; do
    [ -e "$archivo" ] || continue
    python3 -c "
import json, sys

with open('$archivo') as f:
    data = json.load(f)

# Extraer etiquetas y métricas
metrics = data.get('metrics', {})
post_p95 = metrics.get('post_jobs_duration', {}).get('values', {}).get('p(95)', 0.0)
total_p95 = metrics.get('job_total_duration', {}).get('values', {}).get('p(95)', 0.0) / 1000.0
err_rate = metrics.get('slo_unexpected_errors', {}).get('values', {}).get('rate', 0.0) * 100.0

slo1_ok = '✅' if post_p95 <= 1000 else '❌'
slo2_ok = '✅' if total_p95 <= 30.0 else '❌'
slo3_ok = '✅' if err_rate < 1.0 else '❌'
global_ok = '**CUMPLE**' if (post_p95 <= 1000 and total_p95 <= 30.0 and err_rate < 1.0) else '**NO CUMPLE**'

nombre = sys.argv[1].replace('celda_', '').replace('.json', '')
print(f'| \`{nombre}\` | {post_p95:.1f} ms | {total_p95:.2f} s | {err_rate:.2f}% | {slo1_ok} | {slo2_ok} | {slo3_ok} | {global_ok} |')
" "$(basename "$archivo")" >> "$informe_md" 2>/dev/null || true
  done

  echo "" >> "$informe_md"
  echo "Informe consolidado exportado a: $informe_md"
}

# 6. Modo Matriz Completa
correr_matriz() {
  asegurar_datos
  determinar_k6

  echo "========================================================================"
  echo " Iniciando ejecución representativa de la Matriz R5"
  echo " Cruces de VA (1, 10, 50) x VB (1, 2, 3) x VC (1, 10, 50) x V1 (1mb, 10mb)"
  echo "========================================================================"

  # Celdas representativas de la matriz R5:
  # 1. Carga liviana: VA=1, VB=1, VC=1, V1=1mb
  correr_celda 1 1 1 "1mb" "15s"

  # 2. Concurrencia de usuarios: VA=1, VB=2, VC=10, V1=1mb
  correr_celda 1 2 10 "1mb" "20s"

  # 3. Múltiples operaciones y archivos: VA=10, VB=2, VC=10, V1=1mb
  correr_celda 10 2 10 "1mb" "30s"

  # 4. Pipeline completo (3 operaciones): VA=10, VB=3, VC=10, V1=1mb
  correr_celda 10 3 10 "1mb" "30s"

  # 5. Alta concurrencia de usuarios: VA=1, VB=1, VC=50, V1=1mb
  correr_celda 1 1 50 "1mb" "30s"

  # 6. Gran volumen de archivos: VA=50, VB=1, VC=10, V1=1mb
  correr_celda 50 1 10 "1mb" "45s"

  # 7. Imagen pesada (V1 = 10mb): VA=1, VB=2, VC=10, V1=10mb
  correr_celda 1 2 10 "10mb" "45s"

  generar_informe_md
}

# Parseo de opciones
case "${1:-ayuda}" in
  --smoke)
    correr_smoke
    ;;
  --celda)
    asegurar_datos
    determinar_k6
    VA="${2:-1}"
    VB="${3:-1}"
    VC="${4:-1}"
    V1="${5:-1mb}"
    correr_celda "$VA" "$VB" "$VC" "$V1"
    generar_informe_md
    ;;
  --matriz)
    correr_matriz
    ;;
  *)
    echo "Uso: $0 [OPCIÓN]"
    echo ""
    echo "Opciones:"
    echo "  --smoke                  Ejecuta el smoke test de validación de contrato"
    echo "  --celda VA VB VC V1      Ejecuta una celda específica de la matriz"
    echo "                           Ejemplo: $0 --celda 10 2 10 1mb"
    echo "  --matriz                 Ejecuta la corrida representativa de la matriz R5"
    echo ""
    echo "Variables de entorno opcionales:"
    echo "  TARGET_URL   URL base del backend (default: http://localhost:8080)"
    echo "  V3           Cantidad de workers activos (default: 2)"
    echo "  DURATION     Duración de carga por celda (default: 30s)"
    echo "  RAMP_UP      Tiempo de ramp-up (default: 5s)"
    exit 0
    ;;
esac
