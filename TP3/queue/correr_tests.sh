#!/usr/bin/env bash
# ==============================================================================
# correr_tests.sh — Runner Automatizado de Tests Unitarios de Hit #0 (Parte 1 Colas)
# Sistemas Distribuidos y Paralelos — Grupo Cerberus (Salva / Salvador Baez)
# ==============================================================================
# Propósito del script:
# Este script ejecuta secuencialmente los tests unitarios de los 4 ejercicios del
# Hit #0 (ex1: colas básicas, ex2: workers concurrentes, ex3: pub/sub con fanout,
# ex4: direct exchange y enrutamiento con routing keys).
#
# Aspecto arquitectónico crítico (Problema de colisión en sys.modules):
# En pytest y el motor de importación de Python, cuando se ejecutan tests en un
# mismo proceso, los módulos importados se almacenan en caché dentro de sys.modules.
# Dado que 'ex1/src/consumer.py' y 'ex4/src/consumer.py' comparten el mismo nombre
# de archivo ('consumer.py'), si se corrieran en un solo comando pytest, los tests
# de ex4 importarían inadvertidamente el consumidor de ex1, provocando fallos fantasma.
# Para evitar esto, este script lanza cada suite dentro de una subshell aislada (cd ...)
# garantizando total independencia de procesos.
# ==============================================================================

# Modo estricto de Bash:
# -e: Aborta ante comandos fallidos no capturados.
# -u: Previene el uso de variables no inicializadas.
# -o pipefail: Reporta el código de error exacto dentro de tuberías.
set -euo pipefail

# Obtiene la ruta absoluta del directorio que contiene este script (TP3/queue)
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR_RAIZ="$(cd "$DIR/../.." && pwd)"

# Selección inteligente del intérprete de Python:
# 1. Si se definió explícitamente la variable PYTHON, se respeta.
# 2. Si no, o si es 'python3' pero el sistema no tiene pytest, busca el entorno virtual .venv del repo.
PYTHON="${PYTHON:-python3}"
if [ "$PYTHON" = "python3" ]; then
  if ! python3 -m pytest --version >/dev/null 2>&1; then
    if [ -x "$DIR_RAIZ/.venv/bin/python" ]; then
      PYTHON="$DIR_RAIZ/.venv/bin/python"
    fi
  fi
fi

# Normalización de la ruta de Python a ruta canónica absoluta:
# Si el usuario pasó una ruta relativa (ej: .venv/bin/python), al hacer 'cd' a cada
# subdirectorio ex1/ex2... la ruta relativa dejaría de ser válida. Convertirla a
# ruta absoluta resuelve completamente este inconveniente.
if [[ "$PYTHON" = /* ]]; then
  : # La ruta ya es absoluta (/usr/bin/python3, /home/.../.venv/bin/python)
elif [ -f "$PYTHON" ]; then
  PYTHON="$(cd "$(dirname "$PYTHON")" && pwd)/$(basename "$PYTHON")"
fi

# Contador acumulativo de suites fallidas
fallas=0

# Itera sobre los cuatro módulos de ejercicios de colas AMQP / RabbitMQ
for ex in ex1 ex2 ex3 ex4; do
  echo "=================================================="
  echo "🧪 Ejecutando Tests de: TP3/queue/$ex"
  echo "=================================================="
  
  # Ejecución en subshell aislada mediante paréntesis (...):
  # 1. 'cd "$DIR/$ex"': cambia temporalmente al directorio del ejercicio.
  # 2. '"$PYTHON" -m pytest -v tests': corre pytest en modo verbose dentro de ese directorio.
  # 3. Al cerrar el paréntesis, el directorio de trabajo del proceso padre no se altera.
  (cd "$DIR/$ex" && "$PYTHON" -m pytest -v tests) || fallas=$((fallas + 1))
  
  echo ""
done

# Veredicto consolidado de la batería de pruebas de Hit #0
if [ "$fallas" -eq 0 ]; then
  echo "🎉 Todos los tests del Hit #0 pasaron exitosamente (30/30 tests OK)."
else
  echo "❌ Fallaron $fallas suites de prueba en Hit #0."
  exit 1
fi
