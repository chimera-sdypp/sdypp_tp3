#!/usr/bin/env bash
# Cliente de prueba del Hit #2: manda las imágenes de una carpeta a POST /pedidos (en orden, y vuelve a empezar si se
# terminan). Lo normal es fijar el ritmo a mano: escribir los pedidos por minuto y Enter (p. ej. 4.7; + y - suben o
# bajan 0.1), o arrancar con SOBEL_RITMO. Medido con las fotos de 100 MB de la demo, costo 5, 2 locales, umbral 3:
#   X = 3.5   los 2 locales solos (atienden 3.7/min): la cola queda en 0-1, sin VMs;
#   Y = 4.7   la cola llega a 3 en ~2.5 min y lanza una VM (lista en ~60 s); con 3 workers (4.6-4.85/min) queda
#             quieta en 2-3, lejos del umbral de la segunda (6);
#   Z = 5.9   la cola llega a 6 en ~3.5 min y lanza la segunda; con 4 workers (5.8-6.05/min) queda en 4-6.
# Bajando: de Z a Y la segunda VM se borra en ~1 min; de Y a X la primera en ~1 min.
# Sin ritmo a mano hace un pico: un pedido cada SOBEL_ESPERA_PICO s (12) durante SOBEL_DURACION_PICO s (120), después
# la espera crece un 25 % por pedido hasta SOBEL_ESPERA_BAJA s (30), y termina cuando ya no quedan VMs ni cola.
# Muestra cada POST con la respuesta y el estado del master. Ctrl-C para cortar.
#
#   ./cliente.sh <carpeta con imágenes .png o .jpg> [url del master]      (default http://localhost:30080)
#
# SOBEL_COSTO (5): veces que cada worker repite Sobel sobre su franja, para simular una tarea más pesada.
set -euo pipefail
export LC_NUMERIC=C   # EPOCHREALTIME y read -t con punto decimal, también con el locale en español
URL=${2:-http://localhost:30080}
COSTO=${SOBEL_COSTO:-5}
PICO=$((${SOBEL_ESPERA_PICO:-12} * 1000)) DURACION_PICO=${SOBEL_DURACION_PICO:-120}
BAJA=$((${SOBEL_ESPERA_BAJA:-30} * 1000))
imagenes=()
for archivo in "$(cd "$1" && pwd)"/*; do   # .png y .jpg juntos, en orden de nombre
  [[ $archivo == *.png || $archivo == *.jpg ]] && imagenes+=("$archivo")
done
(( ${#imagenes[@]} > 0 )) || { echo "no hay imágenes .png ni .jpg en $1" >&2; exit 2; }
curl -sf "$URL/health" >/dev/null || { echo "el master no responde en $URL/health" >&2; exit 1; }

ms() { local t=${EPOCHREALTIME/./}; echo $((t / 1000)); }
segundos() { printf '%d.%03d' $(($1 / 1000)) $(($1 % 1000)); }
ritmo() { awk -v e="$espera" 'BEGIN { printf "%.2f", 60000 / e }'; }   # pedidos por minuto
valido() { [[ $1 =~ ^[0-9]+([.][0-9]+)?$ ]] && awk -v r="$1" 'BEGIN { exit !(r >= 0.1 && r <= 60) }'; }
a_espera() { awk -v r="$1" 'BEGIN { printf "%d", 60000 / r }'; }

INICIO=$(date +%s)
enviados=0 espera=$PICO fase=pico   # espera entre pedidos, en milisegundos
if [ -n "${SOBEL_RITMO:-}" ]; then
  valido "${SOBEL_RITMO//,/.}" || { echo "SOBEL_RITMO tiene que ser un número de pedidos por minuto (0.1 a 60)" >&2; exit 2; }
  espera=$(a_espera "${SOBEL_RITMO//,/.}") fase=a-mano
fi
[ -t 0 ] && echo "ritmo: escribí los pedidos por minuto y Enter (p. ej. 3.25; + y - suben o bajan 0.1) · Ctrl-C corta"
while true; do
  enviado=$(ms)
  imagen=${imagenes[enviados % ${#imagenes[@]}]}
  respuesta=$(curl -s -w '\n%{http_code}' --data-binary "@$imagen" \
    "$URL/pedidos?costo=$COSTO&nombre=$(basename "$imagen")") || respuesta=$'{}\n000'
  codigo=${respuesta##*$'\n'} cuerpo=${respuesta%$'\n'*}
  enviados=$((enviados + 1))
  json=$(curl -s "$URL/estado" || echo '{}')
  estado=$(jq -r '"en cola \(.en_cola) · VMs \(.vms.listas | length)/\(.vms.pedidas)" +
    " · \(.atendidos_por_s) atendidos/s · latencia \(.latencia_media_s // "-") s"' <<< "$json" 2>/dev/null || echo "sin estado")
  printf '%4d s | POST /pedidos %-12s → %s %s · %s (%s por minuto) | %s\n' $(($(date +%s) - INICIO)) \
    "$(basename "$imagen")" "$codigo" "$(jq -r '.id // .error // ""' <<< "$cuerpo" 2>/dev/null)" "$fase" \
    "$(ritmo)" "$estado"
  if [ "$fase" = valle ] && [ "$(jq -r '"\(.vms.pedidas) \(.en_cola)"' <<< "$json" 2>/dev/null)" = "0 0" ]; then
    echo "──── $enviados pedidos en $(($(date +%s) - INICIO)) s: ya no quedan VMs ni pedidos esperando ────"
    exit 0
  fi

  # Espera al próximo pedido atendiendo las teclas: un ritmo nuevo corre desde el último pedido enviado.
  proximo=$((enviado + espera))
  while (( (falta = proximo - $(ms)) > 0 )); do
    [ -t 0 ] || { sleep "$(segundos "$falta")"; continue; }
    read -r -t "$(segundos "$falta")" linea || continue   # lo que se escribe a medias queda para la próxima lectura
    linea=${linea//[[:space:]]/} linea=${linea//,/.}
    case $linea in
      '') continue ;;
      +)  nuevo=$(awk -v r="$(ritmo)" 'BEGIN { printf "%.2f", r + 0.1 }') ;;
      -)  nuevo=$(awk -v r="$(ritmo)" 'BEGIN { printf "%.2f", r - 0.1 }') ;;
      *)  nuevo=$linea ;;
    esac
    if ! valido "$nuevo"; then
      echo "       \"$linea\" no es un ritmo: escribí los pedidos por minuto, de 0.1 a 60 (p. ej. 3.25)"
      continue
    fi
    espera=$(a_espera "$nuevo") fase=a-mano proximo=$((enviado + espera))
    echo "       ritmo a mano: $(ritmo) pedidos por minuto (un pedido cada $(segundos "$espera") s)"
  done
  case $fase in
    pico)    (( $(date +%s) - INICIO >= DURACION_PICO )) && fase=bajada ;;
    bajada)  espera=$((espera + espera / 4))   # 25 % más
             (( espera >= BAJA )) && { espera=$BAJA fase=valle; } ;;
  esac
done
