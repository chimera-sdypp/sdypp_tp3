#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=========================================================="
echo "  Hit #0 - Ejemplo 1: Message Queue (Punto a Punto)"
echo "=========================================================="

echo "[1/5] Levantando RabbitMQ..."
docker compose up -d rabbitmq
echo "Esperando que RabbitMQ esté listo..."
until docker compose exec -T rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1; do
    sleep 1
done
echo "RabbitMQ está saludable."

echo ""
echo "[2/5] Levantando 1 solo consumidor..."
docker compose up -d --scale consumer=1 consumer
sleep 2

echo ""
echo "[3/5] Ejecutando productor (10 tareas con 1 consumidor)..."
docker compose run --rm producer
sleep 6

echo ""
echo "--- Logs de Consumidor (1 worker atendiendo todas las tareas): ---"
docker compose logs consumer --tail=20

echo ""
echo "[4/5] Escalando a 2 consumidores concurrentes (Round-Robin)..."
docker compose up -d --scale consumer=2 consumer
sleep 2

echo ""
echo "[5/5] Ejecutando productor nuevamente (10 tareas con 2 consumidores)..."
docker compose run --rm producer
sleep 6

echo ""
echo "--- Logs de los 2 Consumidores mostrando Round-Robin: ---"
docker compose logs consumer --tail=30

echo ""
echo "=========================================================="
echo "  Ejecución finalizada con éxito."
echo "  Para apagar los contenedores ejecute: docker compose down -v"
echo "=========================================================="
