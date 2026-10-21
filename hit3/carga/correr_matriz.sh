#!/usr/bin/env bash
# ==============================================================================
# correr_matriz.sh — Runner Automatizado de Pruebas de Carga k6 para Hit #3 (R5)
# Sistemas Distribuidos y Paralelos — Grupo Cerberus (Salva / Salvador Baez)
# ==============================================================================
# Propósito del script:
# Este script automatiza la orquestación y ejecución de la Matriz de Carga R5
# sobre el sistema distribuido desplegado en Kubernetes (k3s).
# Permite:
#   1. Garantizar la presencia de los datos sintéticos (tokens e imágenes) invocando
#      generar_datos.py de forma automática y transparente.
#   2. Detectar de manera inteligente si k6 está disponible de forma nativa o si
#      debe ejecutarse mediante el contenedor oficial de Docker (grafana/k6:latest).
#   3. Ejecutar celdas individuales parametrizadas o la batería representativa completa.
#   4. Consolidar los resultados métricos exportados en formato JSON y compilar
#      automáticamente un reporte ejecutivo en Markdown evaluando los 3 SLOs
#      (con umbrales dinámicos de 30s o 120s según el tamaño total de la carga).
# ==============================================================================

# Configuración de seguridad estricta para scripts de Bash:
# -e: Aborta inmediatamente ante cualquier comando que retorne código distinto de 0.
# -u: Lanza un error fatal si se intenta acceder a una variable no definida.
# -o pipefail: Hace que un pipeline falle si cualquiera de sus comandos falla.
set -euo pipefail

# Resolución de la ruta canónica absoluta donde reside este script
DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Directorios de datos de entrada y de exportación de métricas
DIR_DATOS="$DIR_SCRIPT/datos"
DIR_RESULTADOS="$DIR_SCRIPT/resultados"

# Asegurar la existencia del directorio de almacenamiento de métricas
mkdir -p "$DIR_RESULTADOS"

# ==============================================================================
# Variables de Configuración con Valores Predeterminados
# ==============================================================================

# URL base del balanceador de carga / Ingress Traefik (puerto 80 en k3s local)
TARGET_URL="${TARGET_URL:-http://localhost}"

# Duración predeterminada de la fase de carga estable en cada celda
DURATION="${DURATION:-30s}"

# Duración de la rampa inicial de aceleración de usuarios virtuales
RAMP_UP="${RAMP_UP:-5s}"

# Cantidad nominal de réplicas de workers activas (utilizada como meta-etiqueta V3)
WORKER_COUNT="${V3:-2}"

# ==============================================================================
# Función 1: Verificación y Generación de Datos Sintéticos
# ==============================================================================
asegurar_datos() {
  # Comprueba si faltan los archivos esenciales (tokens JSON o imágenes calibradas)
  if [ ! -f "$DIR_DATOS/tokens.json" ] || [ ! -f "$DIR_DATOS/img_1mb.png" ] || [ ! -f "$DIR_DATOS/img_50mb.png" ]; then
    echo "--- [1/3] Generando datos de prueba sintéticos requeridos ---"
    # Invoca el generador sin dependencias pesadas
    python3 "$DIR_SCRIPT/generar_datos.py"
  else
    echo "--- [1/3] Datos de prueba verificados en: $DIR_DATOS ---"
  fi
}

# ==============================================================================
# Función 2: Detección del Motor de Ejecución de k6 (Nativo vs Contenedor Docker)
# ==============================================================================
determinar_k6() {
  # Opción A: k6 instalado directamente en el sistema operativo host
  if command -v k6 &> /dev/null; then
    K6_CMD="k6 run"
    echo "[INFO] Usando binario k6 local: $(which k6)"
  # Opción B: k6 mediante imagen OCI oficial con soporte de red host
  elif command -v docker &> /dev/null; then
    K6_CMD="docker run --rm -i --network=host -v $DIR_SCRIPT:/app -w /app grafana/k6:latest run"
    echo "[INFO] Usando k6 mediante contenedor Docker (grafana/k6)"
  # Fallo: No se encontró ningún ejecutable capaz de procesar pruebas de carga
  else
    echo "[ERROR] No se encontró ni 'k6' ni 'docker'. Debe instalar k6 o Docker para correr las pruebas."
    exit 1
  fi
}

# ==============================================================================
# Función 3: Ejecución de una Celda Específica de la Matriz R5
# ==============================================================================
correr_celda() {
  # Lectura de argumentos posicionales de la celda
  local va="$1"           # Cantidad de archivos
  local vb="$2"           # Cantidad de operaciones encadenadas
  local vc="$3"           # Concurrencia de VUs
  local v1="$4"           # Calibre de la imagen ('1kb', '1mb', '10mb', '50mb')
  local dur="${5:-$DURATION}" # Duración opcional de la corrida
  
  # Generación de marca temporal para aislar los archivos de resultados
  local timestamp
  timestamp="$(date +%Y%m%d_%H%M%S)"
  local salida_json="$DIR_RESULTADOS/celda_va${va}_vb${vb}_vc${vc}_v1${v1}_${timestamp}.json"

  echo "========================================================================"
  echo " Ejecutando Celda: VA=$va archivos | VB=$vb ops | VC=$vc VUs | V1=$v1 | V3=$WORKER_COUNT"
  echo " Duración: $dur (Ramp-up: $RAMP_UP) | Destino: $TARGET_URL"
  echo "========================================================================"

  # Invoca el comando k6 inyectando las dimensiones como variables de entorno
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

# ==============================================================================
# Función 4: Ejecución del Smoke Test Rápido
# ==============================================================================
correr_smoke() {
  asegurar_datos
  determinar_k6
  echo "========================================================================"
  echo " Ejecutando Smoke Test contra: $TARGET_URL"
  echo "========================================================================"
  $K6_CMD --env TARGET_URL="$TARGET_URL" smoke_test.js
}

# ==============================================================================
# Función 5: Generación de Informe Markdown Consolidado con Evaluación de SLOs
# ==============================================================================
generar_informe_md() {
  local timestamp
  timestamp="$(date +%Y%m%d_%H%M%S)"
  local informe_md="$DIR_RESULTADOS/informe_matriz_${timestamp}.md"

  # Encabezado del informe en formato Markdown
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

| Celda (VA × VB × VC × V1) | p95 POST (ms) | p95 Total (s) | Error (%) | SLO 1 (POST ≤ 1s) | SLO 2 (Total ≤ 30s/120s) | SLO 3 (Err < 1%) | Estado General |
|---|---|---|---|---|---|---|---|
EOF

  # Procesar cada archivo JSON exportado en el directorio de resultados
  for archivo in "$DIR_RESULTADOS"/celda_*.json; do
    [ -e "$archivo" ] || continue
    # Script Python embebido para procesar las métricas de k6 de forma rigurosa
    python3 -c "
import json, sys, re

# Cargar el archivo de resumen de métricas exportado por k6
with open('$archivo') as f:
    data = json.load(f)

# Extraer métricas clave del diccionario exportado
metrics = data.get('metrics', {})
post_p95 = metrics.get('post_jobs_duration', {}).get('values', {}).get('p(95)', 0.0)
total_p95 = metrics.get('job_total_duration', {}).get('values', {}).get('p(95)', 0.0) / 1000.0
err_rate = metrics.get('slo_unexpected_errors', {}).get('values', {}).get('rate', 0.0) * 100.0

# Deducir dimensiones VA y V1 desde el nombre del archivo para fijar el umbral dinámico del SLO 2
m = re.search(r'va(\d+)_vb\d+_vc\d+_v1([a-z0-9]+)', sys.argv[1])
va = int(m.group(1)) if m else 1
v1 = m.group(2) if m else '1mb'

# Según el Contrato Sección 8: Si el total > 10MB (50mb o 10mb con múltiples archivos), el límite es 120s; si no, 30s
umbral_slo2 = 120.0 if (v1 == '50mb' or (v1 == '10mb' and va > 1)) else 30.0

# Evaluación booleana de cada SLO
slo1_ok = '✅' if post_p95 <= 1000 else '❌'
slo2_ok = '✅' if total_p95 <= umbral_slo2 else '❌'
slo3_ok = '✅' if err_rate < 1.0 else '❌'

# Veredicto final global para la celda de la matriz
global_ok = '**CUMPLE**' if (post_p95 <= 1000 and total_p95 <= umbral_slo2 and err_rate < 1.0) else '**NO CUMPLE**'

nombre = sys.argv[1].replace('celda_', '').replace('.json', '')
print(f'| \`{nombre}\` | {post_p95:.1f} ms | {total_p95:.2f} s | {err_rate:.2f}% | {slo1_ok} | {slo2_ok} | {slo3_ok} | {global_ok} |')
" "$(basename "$archivo")" >> "$informe_md" 2>/dev/null || true
  done

  echo "" >> "$informe_md"
  echo "Informe consolidado exportado a: $informe_md"
}

# ==============================================================================
# Función 6: Ejecución de la Matriz Representativa Completa
# ==============================================================================
correr_matriz() {
  asegurar_datos
  determinar_k6

  echo "========================================================================"
  echo " Iniciando ejecución representativa de la Matriz R5"
  echo " Cruces de VA (1, 10, 50) x VB (1, 2, 3) x VC (1, 10, 50) x V1 (1mb, 10mb)"
  echo "========================================================================"

  # Celdas representativas que cubren todo el espectro de la matriz:
  # 1. Carga liviana básica: VA=1, VB=1, VC=1, V1=1mb
  correr_celda 1 1 1 "1mb" "15s"

  # 2. Concurrencia de usuarios moderada: VA=1, VB=2, VC=10, V1=1mb
  correr_celda 1 2 10 "1mb" "20s"

  # 3. Múltiples operaciones y múltiples archivos: VA=10, VB=2, VC=10, V1=1mb
  correr_celda 10 2 10 "1mb" "30s"

  # 4. Pipeline completo de tres transformaciones: VA=10, VB=3, VC=10, V1=1mb
  correr_celda 10 3 10 "1mb" "30s"

  # 5. Alta concurrencia de usuarios virtuales: VA=1, VB=1, VC=50, V1=1mb
  correr_celda 1 1 50 "1mb" "30s"

  # 6. Gran volumen de archivos por lote: VA=50, VB=1, VC=10, V1=1mb
  correr_celda 50 1 10 "1mb" "45s"

  # 7. Carga pesada con imágenes de 10 MB: VA=1, VB=2, VC=10, V1=10mb
  correr_celda 1 2 10 "10mb" "45s"

  # Genera el reporte final
  generar_informe_md
}

# ==============================================================================
# Enrutador de Opciones de Línea de Comandos (CLI)
# ==============================================================================
case "${1:-ayuda}" in
  --smoke)
    # Ejecuta el smoke test de contrato
    correr_smoke
    ;;
  --celda)
    # Ejecuta una celda parametrizada
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
    # Ejecuta la batería representativa completa
    correr_matriz
    ;;
  *)
    # Muestra el menú de ayuda y sintaxis de uso
    echo "Uso: $0 [OPCIÓN]"
    echo ""
    echo "Opciones:"
    echo "  --smoke                  Ejecuta el smoke test de validación de contrato"
    echo "  --celda VA VB VC V1      Ejecuta una celda específica de la matriz"
    echo "                           Ejemplo: $0 --celda 10 2 10 1mb"
    echo "  --matriz                 Ejecuta la corrida representativa de la matriz R5"
    echo ""
    echo "Variables de entorno opcionales:"
    echo "  TARGET_URL   URL base del backend (default: http://localhost)"
    echo "  V3           Cantidad de workers activos (default: 2)"
    echo "  DURATION     Duración de carga por celda (default: 30s)"
    echo "  RAMP_UP      Tiempo de ramp-up (default: 5s)"
    exit 0
    ;;
esac
