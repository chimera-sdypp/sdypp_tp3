#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=========================================================="
echo "  Hit #0 - Ejemplo 3: Dead Letter Queue (DLQ)"
echo "=========================================================="

echo "[1/4] Levantando RabbitMQ..."
# --wait bloquea hasta que el healthcheck del compose (rabbitmq-diagnostics ping) da healthy
docker compose up -d --wait rabbitmq
echo "RabbitMQ está saludable."

echo ""
echo "[2/4] Levantando el consumidor principal y el consumidor de la DLQ..."
docker compose build consumer
docker compose up -d consumer dlq-consumer
sleep 3

echo ""
echo "[3/4] Ejecutando productor (10 tareas, las #3, #6 y #9 con error=true)..."
docker compose run --rm producer
sleep 7

echo ""
echo "--- Consumidor principal (ACK de las válidas, nack de las que tienen error): ---"
docker compose logs --no-log-prefix consumer --tail=12

echo ""
echo "--- Consumidor de la DLQ (solo recibe las tareas rechazadas): ---"
docker compose logs --no-log-prefix dlq-consumer --tail=3

echo ""
echo "[4/4] Topología declarada en el broker:"
docker compose exec -T rabbitmq rabbitmqctl -q list_queues name arguments messages
docker compose exec -T rabbitmq rabbitmqctl -q list_bindings source_name destination_name routing_key | grep dlx

echo ""
echo "=========================================================="
echo "  Ejecución finalizada con éxito."
echo "  UI de management: http://localhost:15672 (guest/guest)"
echo "  Para apagar los contenedores ejecute: docker compose down -v"
echo "=========================================================="
