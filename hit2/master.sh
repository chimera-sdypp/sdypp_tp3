# Corre el master del Hit #1 como un pod de un solo uso en k3s, con la imagen por stdin y la salida por stdout.
#   master <workers> < entrada > salida
# Le pasa las opciones SOBEL_* que estén definidas (timeout, partes, costo).
master() {
  local opciones=() variable
  for variable in SOBEL_TIMEOUT SOBEL_PARTES SOBEL_COSTO; do
    [ -n "${!variable:-}" ] && opciones+=(--env="$variable=${!variable}")
  done
  kubectl run "sobel-master-$BASHPID" -n sobel-hit1 --rm -i -q --restart=Never --image=sobel-hit1:local \
    --env=SOBEL_WORKERS="$1" "${opciones[@]}" -- python master.py - -
}
