#!/usr/bin/env bash
# Hit #2 con desborde según la demanda. Atiende los pedidos (imágenes) que van llegando a una carpeta, en
# orden y de a uno, con los workers locales del Hit #1 (en k3s). Cada 5 s cuenta los pedidos en espera y ajusta las
# VMs a una cada 2 en espera (con 2 o más), hasta SOBEL_MAX_VMS (4). Sube enseguida; baja cuando la cola lleva
# SOBEL_ESTABILIZACION s (30) pidiendo menos, para no borrar una VM que vuelve a hacer falta al rato. Cada VM que
# responde se suma a los pedidos siguientes; antes de borrar una, se la saca de la lista y se espera al pedido en
# curso, que podría estar usándola. Termina cuando aparece <carpeta>/fin y no queda nada por hacer, o con Ctrl-C;
# en los dos casos borra las VMs.
#
#   ./cola.sh <carpeta de pedidos> <carpeta de salida>        (pedidos.sh genera pedidos de prueba)
#
# Mismos requisitos que desbordar.sh. Por cada pedido deja la imagen y el log del master en la carpeta de salida.
# Con cada pedido terminado muestra los pedidos en cola, los atendidos por segundo en el último minuto y la latencia
# media (de la llegada al resultado); al final, el resumen.
set -euo pipefail
PEDIDOS=$1 SALIDA=$2
AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF=(terraform -chdir="$AQUI/infra")
source "$AQUI/master.sh"
MAX_VMS=${SOBEL_MAX_VMS:-4} ESTABILIZACION=${SOBEL_ESTABILIZACION:-30} PASO=5
# en-curso/: el pedido que se está atendiendo · nube: las VMs que responden · arrancando: las que todavía no ·
# hechos: «fin latencia» de cada pedido terminado
ESTADO=$(mktemp -d)
mkdir -p "$ESTADO/en-curso" "$SALIDA"
touch "$ESTADO/nube" "$ESTADO/arrancando" "$ESTADO/hechos"
shopt -s nullglob
INICIO=$(date +%s)

reloj() { printf '%4d s |' $(($(date +%s) - INICIO)); }
cantidad() { local archivos=("$1"/*.*); echo ${#archivos[@]}; }   # los pedidos; `fin` no tiene extensión

estadisticas() {   # en cola · atendidos por segundo en el último minuto · latencia media
  awk -v ahora="$(date +%s)" -v desde="$INICIO" -v cola="$(cantidad "$PEDIDOS")" '
    { n++; suma += $2; if ($1 > ahora - 60) ultimo++ }
    END { ventana = ahora - desde < 60 ? ahora - desde : 60; if (ventana < 1) ventana = 1
          printf "%d en cola · %.2f atendidos/s · latencia media %.0f s", cola, ultimo / ventana, suma / n }
  ' "$ESTADO/hechos"
}

sacar() {   # sacar <worker> <nube|arrancando>: reescribe el archivo entero, así no se mezcla con otro que agrega
  local resto="$ESTADO/$2.$BASHPID"   # acá: en la redirección de grep, $BASHPID sería el pid de grep
  grep -vxF "$1" "$ESTADO/$2" > "$resto" || true
  mv "$resto" "$ESTADO/$2"
}

vms() {   # deja N VMs; las nuevas se suman a la nube a medida que responden /health
  if ! "${TF[@]}" apply -auto-approve -input=false -var cantidad="$1" > "$ESTADO/terraform.log"; then
    echo "$(reloj) falló terraform apply:" >&2; tail -5 "$ESTADO/terraform.log" >&2; return 1
  fi
  local actuales="" worker
  (( $1 == 0 )) || actuales=$("${TF[@]}" output -raw workers | tr , '\n')   # con 0 VMs la salida es null
  for worker in $(cat "$ESTADO/nube" "$ESTADO/arrancando"); do   # las que ya no existen
    grep -qxF "$worker" <<< "$actuales" || { sacar "$worker" nube; sacar "$worker" arrancando; }
  done
  for worker in $actuales; do
    grep -qxF "$worker" "$ESTADO/nube" "$ESTADO/arrancando" && continue
    echo "$worker" >> "$ESTADO/arrancando"
    (
      while grep -qxF "$worker" "$ESTADO/arrancando"; do   # sale si se borra la VM antes de que responda
        if curl -sf "http://$worker/health" >/dev/null; then
          echo "$worker" >> "$ESTADO/nube"
          sacar "$worker" arrancando
          echo "$(reloj) VM $worker lista: $(wc -l < "$ESTADO/nube") en la nube"
          break
        fi
        sleep 3
      done
    ) &
  done
}

bajar() {   # deja N VMs: saca de la nube las que sobran, espera al pedido en curso y recién ahí las borra
  local worker en_curso
  for worker in $("${TF[@]}" output -raw workers | tr , '\n' | tail -n +$(($1 + 1))); do   # terraform borra las últimas
    sacar "$worker" nube
    sacar "$worker" arrancando
  done
  en_curso=("$ESTADO/en-curso"/*.png)
  while (( ${#en_curso[@]} > 0 )) && [ -e "${en_curso[0]}" ]; do sleep 1; done
  vms "$1"
}

escalar() {   # en segundo plano: ajusta las VMs a los pedidos que esperan
  local pedidas=0 deseadas esperando menos_desde=0 tope=0
  until [ -e "$ESTADO/salir" ]; do
    esperando=$(cantidad "$PEDIDOS")
    deseadas=$(( esperando < 2 ? 0 : (esperando + 1) / 2 ))
    deseadas=$(( deseadas > MAX_VMS ? MAX_VMS : deseadas ))
    if (( deseadas > pedidas )); then
      echo "$(reloj) $esperando pedidos esperando → $deseadas VMs (había $pedidas): terraform apply"
      vms "$deseadas" && pedidas=$deseadas
      menos_desde=0
    elif (( deseadas < pedidas )); then
      # Como la ventana de estabilización del HPA: baja al máximo que se pidió en los últimos ESTABILIZACION s.
      if (( menos_desde == 0 )); then menos_desde=$(date +%s) tope=$deseadas; fi
      tope=$(( deseadas > tope ? deseadas : tope ))
      if (( $(date +%s) - menos_desde >= ESTABILIZACION )); then
        echo "$(reloj) $ESTABILIZACION s con pocos pedidos esperando ($esperando) → $tope VMs (había $pedidas)"
        bajar "$tope" && pedidas=$tope
        menos_desde=0
      fi
    else
      menos_desde=0
    fi
    sleep "$PASO"
  done
}

atender() {   # un pedido: el master con los workers locales más las VMs que ya responden
  local pedido=$1 nombre=${1##*/} workers=sobel-worker:8080 vms inicio
  nombre=${nombre%.*} vms=$(wc -l < "$ESTADO/nube") inicio=$(date +%s)
  (( vms == 0 )) || workers+=",$(paste -sd, "$ESTADO/nube")"
  echo "$(reloj) $nombre empieza tras esperar $((inicio - $(stat -c %Y "$pedido"))) s," \
       "con $LOCALES locales + $vms VMs ($(cantidad "$PEDIDOS") esperando)"
  if master "$workers" < "$pedido" > "$SALIDA/$nombre.png" 2> "$SALIDA/$nombre.log"; then
    echo "$(date +%s) $(($(date +%s) - $(stat -c %Y "$pedido")))" >> "$ESTADO/hechos"
    echo "$(reloj) $nombre listo en $(($(date +%s) - inicio)) s | $(estadisticas)"
  else
    echo "$(reloj) $nombre FALLÓ (ver $SALIDA/$nombre.log)" >&2
  fi
  rm -f "$pedido"
}

terminar() {
  touch "$ESTADO/salir"
  [ -n "${pid_escalar:-}" ] && wait "$pid_escalar"   # deja terminar el apply en curso: comparten el lock del estado
  if (( MAX_VMS > 0 )); then
    echo "teardown: borrando las VMs"
    "${TF[@]}" apply -auto-approve -input=false -lock-timeout=60s -var cantidad=0 > /dev/null ||
      echo "ATENCIÓN: falló el teardown, borrarlas con: terraform -chdir=$AQUI/infra apply -var cantidad=0" >&2
  fi
  rm -rf "$ESTADO"
}
trap terminar EXIT

kubectl rollout status deployment/sobel-worker -n sobel-hit1 --timeout=60s
LOCALES=$(kubectl get deployment/sobel-worker -n sobel-hit1 -o jsonpath='{.spec.replicas}')
escalar & pid_escalar=$!
atendidos=0
while true; do
  pendientes=("$PEDIDOS"/*.*)   # pedido-001, pedido-002…: el glob los ordena por llegada
  if (( ${#pendientes[@]} > 0 )); then
    mv "${pendientes[0]}" "$ESTADO/en-curso/"
    atender "$ESTADO/en-curso/${pendientes[0]##*/}"
    atendidos=$((atendidos + 1))
  elif [ -e "$PEDIDOS/fin" ]; then
    break
  else
    sleep 1
  fi
done
echo "$(reloj) no quedan pedidos: $atendidos atendidos"
awk -v t=$(($(date +%s) - INICIO)) '{ n++; s += $2; if ($2 > m) m = $2 }
  END { if (n) printf "resumen: %d pedidos en %d s · %.3f atendidos/s · latencia media %.0f s (máx. %d s)\n",
                      n, t, n / t, s / n, m }' "$ESTADO/hechos"
