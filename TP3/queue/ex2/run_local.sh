#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=========================================================="
echo "  Hit #0 - Ejemplo 2: Event Bus / Pub-Sub (Fan-out)"
echo "=========================================================="

echo "[1/4] Levantando RabbitMQ..."
# --wait bloquea hasta que el healthcheck del compose (rabbitmq-diagnostics ping) da healthy
docker compose up -d --wait rabbitmq
echo "RabbitMQ está saludable."

echo ""
echo "[2/4] Levantando 3 suscriptores (Nodos de la red: Nodo-Red-1, Nodo-Red-2, Nodo-Red-3)..."
docker compose up -d node-1 node-2 node-3
sleep 3

echo ""
echo "[3/4] Ejecutando publicador de eventos 'Nuevo Bloque Minado' (5 bloques)..."
docker compose run --rm publisher
sleep 3

echo ""
echo "--- Verificación: Bloques recibidos por cada nodo suscriptor: ---"
docker compose logs node-1 node-2 node-3 | grep "¡NUEVO BLOQUE"

echo ""
echo "=========================================================="
echo "  Ejecución finalizada con éxito. Todos los nodos recibieron cada bloque."
echo "  Para apagar los contenedores ejecute: docker compose down -v"
echo "=========================================================="
