#!/usr/bin/env bash
# ==============================================================================
# correr_tests.sh — Runner de tests unitarios del Hit #0 (Parte 1)
# ==============================================================================
# Ejecuta los tests de cada ejemplo (ex1 a ex4) en subprocesos aislados para
# evitar colisiones de importación en sys.modules entre módulos homónimos.
# ==============================================================================
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"
if [[ "$PYTHON" = /* ]]; then
  : # Ya es ruta absoluta
elif [ -f "$PYTHON" ]; then
  PYTHON="$(cd "$(dirname "$PYTHON")" && pwd)/$(basename "$PYTHON")"
fi

fallas=0
for ex in ex1 ex2 ex3 ex4; do
  echo "=================================================="
  echo "🧪 Tests de TP3/queue/$ex"
  echo "=================================================="
  (cd "$DIR/$ex" && "$PYTHON" -m pytest -v tests) || fallas=$((fallas + 1))
  echo ""
done

if [ "$fallas" -eq 0 ]; then
  echo "🎉 Todos los tests del Hit #0 pasaron exitosamente (30/30 tests OK)."
else
  echo "❌ Fallaron $fallas suites de prueba."
  exit 1
fi
