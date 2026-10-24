#!/usr/bin/env bash
# ==============================================================================
# correr_matriz.sh — Runner de las pruebas de carga k6 del Hit #3 (R5)
# Sistemas Distribuidos y Paralelos — Grupo Cerberus
# ==============================================================================
# Modos (ver ./correr_matriz.sh sin argumentos):
#   --smoke                 test de humo del contrato
#   --celda VA VB VC V1     una celda, con los workers que haya
#   --matriz                corrida representativa (8 celdas, una con un worker que muere)
#   --completa              VA × VB × VC × V1_VALORES para cada valor de V3_VALORES (escala el worker con kubectl)
#   --v1                    barrido de tamaño: 1 KB → 100 MB con VA=1, VB=1, VC=1
#
# Cada corrida guarda sus JSON en resultados/<fecha>/ y arma ahí informe.md con los 3 SLOs del contrato (§8),
# así un informe nunca mezcla celdas de corridas distintas.
# ==============================================================================
set -euo pipefail

DIR_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# k6 resuelve open('./datos/...') desde la carpeta del script, y en Docker sólo está montada esa carpeta:
# todas las rutas que se le pasan a k6 son relativas a ella.
cd "$DIR_SCRIPT"

TARGET_URL="${TARGET_URL:-http://localhost}"
DURATION="${DURATION:-30s}"
RAMP_UP="${RAMP_UP:-5s}"
NAMESPACE="${SOBEL_NAMESPACE:-sobel}"
DEPLOY_WORKER="${SOBEL_DEPLOY_WORKER:-sobel-worker}"
V3_VALORES="${V3_VALORES:-2 4}"
V1_VALORES="${V1_VALORES:-1mb 10mb}"
K6_IMAGEN="grafana/k6:1.8.1"
LIMITE_PEDIDO=$((200 * 1024 * 1024))  # contrato: un pedido de más de 200 MiB recibe 413

CORRIDA="resultados/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$CORRIDA"

asegurar_datos() {
  local falta=0
  for archivo in tokens.json corrupta.png img_1kb.png img_1mb.png img_10mb.png img_50mb.png img_100mb.png; do
    [ -f "datos/$archivo" ] || falta=1
  done
  if [ "$falta" -eq 1 ]; then
    echo "--- Generando datos de prueba (tokens e imágenes) ---"
    python3 generar_datos.py
  fi
}

determinar_k6() {
  if command -v k6 &> /dev/null; then
    K6_CMD=(k6 run)
  elif command -v docker &> /dev/null; then
    # -u: los JSON quedan del usuario, no de root
    K6_CMD=(docker run --rm -i --network=host -u "$(id -u):$(id -g)" -v "$DIR_SCRIPT:/app" -w /app "$K6_IMAGEN" run)
  else
    echo "[ERROR] Hace falta k6 o Docker." >&2
    exit 1
  fi
  echo "[INFO] k6: ${K6_CMD[*]}"
}

# Workers listos ahora (V3 de la celda). Sin kubectl se toma $V3 tal cual.
workers_listos() {
  local listos
  if command -v kubectl &> /dev/null &&
     listos=$(kubectl -n "$NAMESPACE" get deploy "$DEPLOY_WORKER" -o jsonpath='{.status.readyReplicas}' 2>/dev/null); then
    echo "${listos:-0}"  # sin réplicas listas, k8s omite el campo
  else
    echo "${V3:-?}"
  fi
}

escalar_workers() {
  local n="$1"
  echo "--- Escalando $DEPLOY_WORKER a $n réplicas ---"
  kubectl -n "$NAMESPACE" scale deploy "$DEPLOY_WORKER" --replicas="$n"
  kubectl -n "$NAMESPACE" rollout status deploy "$DEPLOY_WORKER" --timeout=180s
  sleep 6  # cada worker se anuncia en Redis cada 5 s: sin eso el backend todavía no los ve
}

# Mata un pod del worker en plena meseta de carga (caso "worker que muere" de R5).
matar_un_worker_despues_de() {
  local espera="$1"
  (
    sleep "$espera"
    pod=$(kubectl -n "$NAMESPACE" get pods -l "app.kubernetes.io/name=$DEPLOY_WORKER" -o name | head -1)
    echo "[FALLA] Borrando $pod a mitad de la celda"
    kubectl -n "$NAMESPACE" delete "$pod" --wait=false
  ) &
}

tamano_v1() {
  stat -c %s "datos/img_$1.png"
}

# correr_celda VA VB VC V1 [duración] [matar_worker]
correr_celda() {
  local va="$1" vb="$2" vc="$3" v1="$4" dur="${5:-$DURATION}" matar="${6:-0}"
  local bytes=$((va * $(tamano_v1 "$v1")))
  if [ "$bytes" -gt "$LIMITE_PEDIDO" ]; then
    echo "[SALTEADA] VA=$va × V1=$v1 son $((bytes / 1024 / 1024)) MiB: el contrato lo rechaza con 413"
    return
  fi
  local v3
  v3="$(workers_listos)"
  local nombre="celda_va${va}_vb${vb}_vc${vc}_v1${v1}_v3${v3}"
  [ "$matar" -eq 1 ] && nombre="${nombre}_falla"
  echo "========================================================================"
  echo " VA=$va archivos | VB=$vb ops | VC=$vc VUs | V1=$v1 | V3=$v3 | ${dur} (ramp-up $RAMP_UP)"
  [ "$matar" -eq 1 ] && echo " Con un worker que muere a mitad de la celda"
  echo "========================================================================"
  if [ "$matar" -eq 1 ]; then
    matar_un_worker_despues_de 15
  fi
  # k6 sale con error si no se cumple un threshold: es un resultado de la celda, no un fallo del runner.
  "${K6_CMD[@]}" \
    --env TARGET_URL="$TARGET_URL" --env VA="$va" --env VB="$vb" --env VC="$vc" --env V1="$v1" \
    --env V3="$v3" --env DURATION="$dur" --env RAMP_UP="$RAMP_UP" \
    --summary-export="$CORRIDA/$nombre.json" \
    matriz_r5.js || true
  wait
}

generar_informe() {
  local informe="$CORRIDA/informe.md"
  {
    echo "# Matriz R5 — $(date '+%Y-%m-%d %H:%M')"
    echo
    echo "Destino: \`$TARGET_URL\` · ramp-up $RAMP_UP · meseta $DURATION (salvo que la celda diga otra cosa)."
    echo
    echo "Una celda **cumple** si: p95 de \`POST /jobs\` ≤ 1 s; p95 de \`tiempos_s.total\` ≤ 30 s para pedidos de"
    echo "hasta 10 MiB en total (≤ 120 s si son más grandes); y menos de 1 % de errores no provocados (contrato §8)."
    echo
    echo "| VA | VB | VC | V1 | V3 | Falla | p95 POST (ms) | p95 total (s) | Errores (%) | Req/s | Resultado |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|"
  } > "$informe"
  for archivo in "$CORRIDA"/celda_*.json; do
    [ -e "$archivo" ] || continue
    # --summary-export guarda cada métrica plana: {"p(95)": …} en Trend, {"value": …} en Rate y {"rate": …} en Counter.
    python3 - "$archivo" "$DIR_SCRIPT/datos" >> "$informe" <<'EOF'
import json, os, re, sys
archivo, datos = sys.argv[1], sys.argv[2]
m = json.load(open(archivo))["metrics"]
nombre = os.path.basename(archivo)
va, vb, vc, v1, v3 = re.search(r"va(\d+)_vb(\d+)_vc(\d+)_v1([a-z0-9]+)_v3([0-9?]+)", nombre).groups()
falla = "sí" if "_falla" in nombre else ""
post = m.get("post_jobs_duration", {}).get("p(95)")
total = m.get("job_total_duration", {}).get("p(95)")
errores = m.get("slo_unexpected_errors", {}).get("value")
rps = m.get("http_reqs", {}).get("rate", 0)
umbral = 120 if int(va) * os.path.getsize(f"{datos}/img_{v1}.png") > 10 * 1024 * 1024 else 30
if post is None or total is None or errores is None:
    # Sin muestras no hay medición: no se puede dar por cumplida
    resultado, celdas = "**SIN DATOS**", ["—", "—", "—"]
else:
    ok = post <= 1000 and total / 1000 <= umbral and errores < 0.01
    resultado = "**CUMPLE**" if ok else "**NO CUMPLE**"
    celdas = [f"{post:.0f}", f"{total / 1000:.2f} (≤ {umbral})", f"{errores * 100:.2f}"]
print(f"| {va} | {vb} | {vc} | {v1} | {v3} | {falla} | {' | '.join(celdas)} | {rps:.2f} | {resultado} |")
EOF
  done
  echo "Informe: $DIR_SCRIPT/$informe"
}

correr_matriz() {
  correr_celda 1 1 1 1mb 15s
  correr_celda 1 2 10 1mb 20s
  correr_celda 10 2 10 1mb
  correr_celda 10 3 10 1mb
  correr_celda 1 1 50 1mb
  correr_celda 50 1 10 1mb 45s
  correr_celda 1 2 10 10mb 45s
  correr_celda 10 2 10 1mb 60s 1
}

correr_completa() {
  for v3 in $V3_VALORES; do
    escalar_workers "$v3"
    for v1 in $V1_VALORES; do
      for va in 1 10 50; do
        for vb in 1 2 3; do
          for vc in 1 10 50; do
            correr_celda "$va" "$vb" "$vc" "$v1"
          done
        done
      done
    done
  done
}

correr_v1() {
  for v1 in 1kb 1mb 10mb 50mb 100mb; do
    correr_celda 1 1 1 "$v1"
  done
}

case "${1:-ayuda}" in
  --smoke)
    asegurar_datos; determinar_k6
    "${K6_CMD[@]}" --env TARGET_URL="$TARGET_URL" smoke_test.js
    ;;
  --celda)
    asegurar_datos; determinar_k6
    correr_celda "${2:-1}" "${3:-1}" "${4:-1}" "${5:-1mb}"
    generar_informe
    ;;
  --matriz)
    asegurar_datos; determinar_k6; correr_matriz; generar_informe
    ;;
  --completa)
    asegurar_datos; determinar_k6; correr_completa; generar_informe
    ;;
  --v1)
    asegurar_datos; determinar_k6; correr_v1; generar_informe
    ;;
  *)
    sed -n '6,11p' "$0" | sed 's/^# \?//'
    echo
    echo "Variables: TARGET_URL (default http://localhost), DURATION (30s), RAMP_UP (5s),"
    echo "  V3_VALORES (\"2 4\"), V1_VALORES (\"1mb 10mb\"), SOBEL_NAMESPACE (sobel), SOBEL_DEPLOY_WORKER (sobel-worker)"
    rmdir "$CORRIDA" 2>/dev/null || true
    ;;
esac
