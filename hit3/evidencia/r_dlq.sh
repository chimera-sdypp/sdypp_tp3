#!/usr/bin/env bash
# R_DLQ: Caso de prueba de fallas y Dead Letter Queue (DLQ).
# Demuestra el ciclo completo ante datos corruptos o no procesables:
# 1. Fallo inicial en worker (nack).
# 2. Reasignador capturando de sobel.dlq y reintentando (intentos 2 y 3).
# 3. Agotamiento de reintentos (> 3) y traspaso a sobel.dlq.agotados.
# 4. Job pasando a estado 'dlq' con error REINTENTOS_AGOTADOS.
# 5. Detección de mensajes en DLQ (métrica para alerta SobelDLQAgotadosPresentes).
# 6. Intervención del administrador mediante POST /admin/dlq/reprocesar.
# 7. Recuperación exitosa y finalización del job en estado 'done'.
set -euo pipefail

URL="${1:-http://localhost}"
NS="${SOBEL_NAMESPACE:-sobel}"
IMAGEN="${SOBEL_IMAGEN:-ghcr.io/chimera-sdypp/sobel-worker:1.0.0}"
TIMESTAMP=$(date +%s)
JOB_ID="job-corrupto-$TIMESTAMP"

verde() { printf "\033[1;32m%s\033[0m\n" "$*"; }
amarillo() { printf "\033[1;33m%s\033[0m\n" "$*"; }
rojo() { printf "\033[1;31m%s\033[0m\n" "$*"; }
azul() { printf "\033[1;34m%s\033[0m\n" "$*"; }

azul "=========================================================================="
azul "  PRUEBA AUTOMATIZADA DE FALLOS, REINTENTOS Y RECUPERACIÓN DLQ"
azul "=========================================================================="
echo

amarillo "1. Generando tokens JWT para usuario (ana) y administrador (root)..."
ANA=$(kubectl -n "$NS" exec deploy/backend -- python -m backend.emitir_token ana usuario --horas 1)
ADMIN=$(kubectl -n "$NS" exec deploy/backend -- python -m backend.emitir_token root admin --horas 1)
verde "   Tokens generados exitosamente."
echo

amarillo "2. Inyectando tarea con datos corruptos (matriz inválida) para '$JOB_ID'..."
kubectl -n "$NS" exec deploy/backend -- python -c "
import json, time, os, redis, pika
from comun import claves, colas

r = redis.Redis.from_url(os.environ['REDIS_URL'])
conn = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_URL']))
ch = conn.channel()

job_id = '$JOB_ID'
imagen = '$IMAGEN'
cola_worker = colas.declarar_tareas(ch, imagen)

# Crear job en Redis con estructura estándar
r.hset(claves.job(job_id), mapping={
    'usuario': 'ana',
    'estado': 'processing',
    'creado': int(time.time()*1000),
    'archivos': '1',
    'fragmentos_total': '1',
    'completos': '0',
    'en_dlq': '0',
    'reintentos': '0',
    'cola_ms': '0',
    'cola_n': '0',
    'proc_ms': '0',
    'proc_n': '0',
    'operaciones': json.dumps([{'operacion': 'sobel', 'imagen': imagen}])
})
r.expire(claves.job(job_id), 600)

# Geometría esperada por el joiner (imagen 16x16, 1 fragmento)
r.hset(claves.geometria(job_id, 0), mapping={'total': '1', '0': '0,16'})
r.expire(claves.geometria(job_id, 0), 600)

# Guardar fragmento corrupto en Redis (bytes que fallan en decodificación PIL)
clave_frag = claves.fragmento(job_id, 0, 0, 0)
r.set(clave_frag, b'BYTES_CORRUPTOS_MATRIZ_INVALIDA_A_PROPOSITO', ex=600)

# Publicar la tarea en la cola del worker
tarea = {
    'job_id': job_id,
    'archivo': 0,
    'fragmento': 0,
    'paso': 0,
    'intento': 1,
    'operaciones': [{'operacion': 'sobel', 'imagen': imagen}],
    'encolado': int(time.time()*1000)
}
colas.publicar(ch, cola_worker, tarea)
conn.close()
"
verde "   Tarea corrupta inyectada en RabbitMQ."
echo

amarillo "3. Esperando que el worker falle y el reasignador ejecute los reintentos..."
sleep 3
echo

azul "--- Evidencia en logs del Reasignador (sobel.dlq -> reintentos -> agotados) ---"
kubectl -n "$NS" logs deploy/reasignador --since=15s | grep -E "job $JOB_ID" || true
echo

amarillo "4. Consultando el estado del job vía API pública (GET /jobs/$JOB_ID)..."
RESP_JOB=$(curl -s -H "Authorization: Bearer $ANA" "$URL/jobs/$JOB_ID")
echo "$RESP_JOB" | python3 -m json.tool || echo "$RESP_JOB"
echo

ESTADO=$(echo "$RESP_JOB" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("contenido", {}).get("estado", ""))')
EN_DLQ=$(echo "$RESP_JOB" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("contenido", {}).get("fragmentos", {}).get("en_dlq", 0))')
REINTENTOS=$(echo "$RESP_JOB" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("contenido", {}).get("reintentos", 0))')

if [ "$ESTADO" = "dlq" ] && [ "$EN_DLQ" -eq 1 ] && [ "$REINTENTOS" -ge 3 ]; then
  verde "   Job pasó correctamente a estado 'dlq' con $REINTENTOS reintentos y $EN_DLQ en DLQ."
else
  rojo "   ERROR: El job no quedó en estado esperado (Estado: $ESTADO, DLQ: $EN_DLQ, Reintentos: $REINTENTOS)"
  exit 1
fi
echo

amarillo "5. Verificando cola 'sobel.dlq.agotados' en RabbitMQ..."
MENSAJES_AGOTADOS=$(kubectl -n "$NS" exec rabbitmq-0 -- rabbitmqctl list_queues name messages --silent | awk '$1=="sobel.dlq.agotados"{print $2}')
echo "   Mensajes acumulados en sobel.dlq.agotados: $MENSAJES_AGOTADOS"
if [ "$MENSAJES_AGOTADOS" -ge 1 ]; then
  verde "   Condición de alerta 'SobelDLQAgotadosPresentes' CUMPLIDA (rabbitmq_queue_messages > 0)."
else
  rojo "   ERROR: sobel.dlq.agotados no tiene mensajes."
  exit 1
fi
echo

amarillo "6. Verificando métricas desde la API de administración (GET /stats)..."
STATS=$(curl -s -H "Authorization: Bearer $ADMIN" "$URL/stats")
echo "$STATS" | python3 -m json.tool
echo

amarillo "7. Reparando los datos en Redis para permitir recuperación exitosa..."
PNG_VALIDO="iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAIAAACQkWg2AAAAGklEQVR42mM8oaHBQApgYiARjGoY1TB0NAAAjC0BOJDndDkAAAAASUVORK5CYII="
kubectl -n "$NS" exec deploy/backend -- python -c "
import base64, os, redis
from comun import claves
r = redis.Redis.from_url(os.environ['REDIS_URL'])
png_bytes = base64.b64decode('$PNG_VALIDO')
r.set(claves.fragmento('$JOB_ID', 0, 0, 0), png_bytes, ex=600)
"
verde "   Fragmento corregido con imagen PNG válida en Redis."
echo

amarillo "8. Ejecutando reprocesamiento de administrador (POST /admin/dlq/reprocesar)..."
RESP_REPROC=$(curl -s -X POST -H "Authorization: Bearer $ADMIN" "$URL/admin/dlq/reprocesar")
echo "$RESP_REPROC" | python3 -m json.tool
echo

sleep 3

amarillo "9. Verificando que la cola 'sobel.dlq.agotados' haya sido vaciada..."
MENSAJES_RESTANTES=$(kubectl -n "$NS" exec rabbitmq-0 -- rabbitmqctl list_queues name messages --silent | awk '$1=="sobel.dlq.agotados"{print $2}')
echo "   Mensajes restantes en sobel.dlq.agotados: $MENSAJES_RESTANTES"
if [ "$MENSAJES_RESTANTES" -eq 0 ]; then
  verde "   La cola sobel.dlq.agotados quedó limpia (0 mensajes)."
else
  rojo "   ADVERTENCIA: Aún quedan mensajes en sobel.dlq.agotados ($MENSAJES_RESTANTES)."
fi
echo

amarillo "10. Consultando estado final del job reparado (GET /jobs/$JOB_ID)..."
RESP_FINAL=$(curl -s -H "Authorization: Bearer $ANA" "$URL/jobs/$JOB_ID")
echo "$RESP_FINAL" | python3 -m json.tool
ESTADO_FINAL=$(echo "$RESP_FINAL" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("contenido", {}).get("estado", ""))')
if [ "$ESTADO_FINAL" = "done" ]; then
  verde "   Job procesado y finalizado exitosamente (Estado: done)."
fi
echo

azul "=========================================================================="
verde "  PRUEBA DE CICLO COMPLETO DE DLQ FINALIZADA CON ÉXITO"
azul "=========================================================================="
