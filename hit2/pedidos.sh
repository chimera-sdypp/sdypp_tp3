#!/usr/bin/env bash
# Pedidos de prueba para cola.sh: deja en <carpeta de pedidos> un enlace a cada imagen de <carpeta con imágenes>
# (pedido-001.jpg, pedido-002.png…; si se acaban, vuelve a empezar). Al terminar crea <carpeta de pedidos>/fin, que le
# avisa a cola.sh que no llegan más. El ritmo sale de SOBEL_RITMO:
#   rampa (default)  cada vez más seguidos: espera SOBEL_ESPERA_INICIAL s (60) y cada vez un 20 % menos, hasta
#                    SOBEL_ESPERA_MINIMA (8).
#   pico             uno cada SOBEL_ESPERA_PICO s (7) durante SOBEL_DURACION_PICO s (180); después la espera crece un
#                    25 % por pedido hasta SOBEL_ESPERA_BAJA (20) y sigue así hasta completar la cantidad.
#
#   ./pedidos.sh <cantidad> <carpeta con imágenes .png o .jpg> <carpeta de pedidos>
set -euo pipefail
CANTIDAD=$1 PEDIDOS=$3 RITMO=${SOBEL_RITMO:-rampa}
case $RITMO in
  rampa) espera=${SOBEL_ESPERA_INICIAL:-60} minima=${SOBEL_ESPERA_MINIMA:-8} ;;
  pico)  espera=${SOBEL_ESPERA_PICO:-7} duracion=${SOBEL_DURACION_PICO:-180} baja=${SOBEL_ESPERA_BAJA:-20} ;;
  *) echo "SOBEL_RITMO tiene que ser rampa o pico" >&2; exit 2 ;;
esac
imagenes=()
for archivo in "$(cd "$2" && pwd)"/*; do   # .png y .jpg juntos, en orden de nombre
  [[ $archivo == *.png || $archivo == *.jpg ]] && imagenes+=("$archivo")
done
mkdir -p "$PEDIDOS"
INICIO=$(date +%s)
for ((i = 1; i <= CANTIDAD; i++)); do
  imagen=${imagenes[(i - 1) % ${#imagenes[@]}]}
  ln -s "$imagen" "$PEDIDOS/$(printf 'pedido-%03d' "$i").${imagen##*.}"
  if [ "$RITMO" = pico ] && (( $(date +%s) - INICIO >= duracion )); then
    espera=$(( espera + (espera >= 4 ? espera / 4 : 1) ))   # 25 % más, y al menos 1 s
    espera=$(( espera < baja ? espera : baja ))
  fi
  # El %.1f lo hace awk: el printf de bash con el locale en español no acepta "8.6".
  printf '%4d s | llega el pedido-%03d · uno cada %d s (%s por minuto)\n' $(($(date +%s) - INICIO)) "$i" "$espera" \
    "$(awk -v e="$espera" 'BEGIN { printf "%.1f", 60 / e }')"
  (( i < CANTIDAD )) || break
  sleep "$espera"
  if [ "$RITMO" = rampa ]; then
    espera=$(( espera * 4 / 5 > minima ? espera * 4 / 5 : minima ))
  fi
done
touch "$PEDIDOS/fin"
