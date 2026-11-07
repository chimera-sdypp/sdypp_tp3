#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

echo "=========================================================="
echo "  Hit #0 - Ejemplo 4: Retry con Exponential Backoff"
echo "=========================================================="

echo "[1/4] Levantando RabbitMQ..."
# --wait bloquea hasta que el healthcheck del compose (rabbitmq-diagnostics ping) da healthy
docker compose up -d --wait rabbitmq
echo "RabbitMQ está saludable."

echo ""
echo "[2/4] Levantando el consumidor con retry y el consumidor de la DLQ..."
docker compose build consumer
docker compose up -d consumer dlq-consumer
sleep 3

echo ""
echo "[3/4] Ejecutando productor (10 tareas con P(fallo)=0.5; #5 y #10 con force_fail=true)..."
docker compose run --rm producer
# El peor caso es 1 + 2 + 4 + 8 = 15s de backoff más el procesamiento de cada intento
echo "Esperando 25s a que se agoten los reintentos..."
sleep 25

echo ""
echo "--- Ciclo de vida de la tarea #5 (siempre falla: 5 intentos y DLQ): ---"
docker compose logs --no-log-prefix consumer | grep "Tarea #5 " || true

echo ""
echo "--- Registro completo de intentos y esperas: ---"
docker compose logs --no-log-prefix consumer | grep -E "📥|✅|🔁|🪦" || true

echo ""
echo "--- Consumidor de la DLQ: ---"
docker compose logs --no-log-prefix dlq-consumer | grep "Mensaje fallido" || true

echo ""
echo "[4/4] Topología declarada en el broker:"
docker compose exec -T rabbitmq rabbitmqctl -q list_queues name arguments messages
docker compose exec -T rabbitmq rabbitmqctl -q list_bindings source_name destination_name routing_key | grep -E "dlx|wait"

echo ""
echo "=========================================================="
echo "  Ejecución finalizada con éxito."
echo "  UI de management: http://localhost:15672 (usuario y contraseña de TP3/queue/.env)"
echo "  Para apagar los contenedores ejecute: docker compose down -v"
echo "=========================================================="
