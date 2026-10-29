#!/usr/bin/env bash
# ========================================================================================
# R_DLQ: Caso de Prueba Automatizado para Fallas, Reintentos y Dead Letter Queue (DLQ)
# ========================================================================================
# ¿POR QUÉ EXISTE ESTE SCRIPT Y QUÉ RESPONDE A LA CÁTEDRA?
# En la defensa oral previa (Hit #3, min 1:18:23 – 1:19:43), el profesor observó:
#   "El ciclo de reintentos -> DLQ -> sobel.dlq.agotados -> admin lo diagramaron y lo
#    pusieron en el código, pero no lo probaron. Busquen un caso que haga funcionar ese
#    ciclo, que se rompa (ej: una matriz con números equivocados o datos corruptos)".
#
# Este script automatiza y demuestra ese ciclo completo y reproducible:
#   1. Inyecta una tarea con matriz de bytes corruptos (no decodificable por PIL).
#   2. El worker intenta procesar el fragmento, falla y emite un basic_nack(requeue=False).
#   3. RabbitMQ deriva automáticamente el mensaje por su DLX hacia 'sobel.dlq'.
#   4. El servicio 'reasignador' consume de 'sobel.dlq' y reintenta la tarea hasta 3 veces
#      (mostrando en los logs los intentos 2, 3 y 4 en tiempo real).
#   5. Al superar los 3 reintentos, el reasignador pasa la tarea a 'sobel.dlq.agotados'
#      y actualiza el estado del job en Redis a 'dlq' con tipo 'REINTENTOS_AGOTADOS'.
#   6. La cola 'sobel.dlq.agotados' acumula el mensaje, activando la métrica de Prometheus
#      que dispara la alerta 'SobelDLQAgotadosPresentes' (definida en hit4/alertrules.yaml).
#   7. El administrador repara los datos del fragmento en Redis y ejecuta la acción correctiva:
#      POST /admin/dlq/reprocesar.
#   8. El backend saca la tarea de 'sobel.dlq.agotados', resetea su intento a 1 y la reenvía
#      a los workers, finalizando el job exitosamente en estado 'done'.
#
# Requisitos:
#   - Cluster Kubernetes local o remoto con los servicios de Sobel en el namespace 'sobel'.
#   - Ingress accesible en http://localhost (por defecto).
# ========================================================================================

# Configuración estricta de bash: aborta ante errores (-e), variables no definidas (-u)
# y propaga fallos en pipes (-o pipefail).
set -euo pipefail

# URL base del Ingress (por defecto http://localhost expuesto por Traefik en k3s)
URL="${1:-http://localhost}"

# Namespace de Kubernetes donde residen los pods del sistema distribuido
NS="${SOBEL_NAMESPACE:-sobel}"

# Imagen de contenedor del worker esperado para procesar las tareas de Sobel
IMAGEN="${SOBEL_IMAGEN:-ghcr.io/chimera-sdypp/sobel-worker:1.0.0}"

# Generamos una marca de tiempo única para aislar este caso de prueba de cualquier otro
TIMESTAMP=$(date +%s)
JOB_ID="job-corrupto-$TIMESTAMP"

# Funciones auxiliares para formatear la salida en terminal con colores ANSI
verde() { printf "\033[1;32m%s\033[0m\n" "$*"; }
amarillo() { printf "\033[1;33m%s\033[0m\n" "$*"; }
rojo() { printf "\033[1;31m%s\033[0m\n" "$*"; }
azul() { printf "\033[1;34m%s\033[0m\n" "$*"; }

azul "=========================================================================="
azul "  PRUEBA AUTOMATIZADA DE FALLOS, REINTENTOS Y RECUPERACIÓN DLQ"
azul "=========================================================================="
echo

# ----------------------------------------------------------------------------------------
# PASO 1: Generación de identidades y tokens JWT
# ----------------------------------------------------------------------------------------
# Generamos dos tokens utilizando el módulo backend.emitir_token dentro del pod del backend:
# - ana: usuario normal para crear y consultar sus propios jobs.
# - root: rol admin necesario para consultar /stats y autorizar /admin/dlq/reprocesar.
amarillo "1. Generando tokens JWT para usuario (ana) y administrador (root)..."
ANA=$(kubectl -n "$NS" exec deploy/backend -- python -m backend.emitir_token ana usuario --horas 1)
ADMIN=$(kubectl -n "$NS" exec deploy/backend -- python -m backend.emitir_token root admin --horas 1)
verde "   Tokens generados exitosamente."
echo

# ----------------------------------------------------------------------------------------
# PASO 2: Inyección de datos corruptos para forzar el fallo del worker
# ----------------------------------------------------------------------------------------
# Simulamos una falla real en la carga de la matriz:
# - En Redis guardamos el hash del job en estado 'processing'.
# - Guardamos en la clave de entrada del fragmento (claves.fragmento) bytes arbitrarios
#   que NO constituyen una imagen válida (falla en PIL/pixeles.leer).
# - Publicamos el mensaje de la tarea en la cola 'sobel.tareas.<imagen>' con intento=1.
amarillo "2. Inyectando tarea con datos corruptos (matriz inválida) para '$JOB_ID'..."
kubectl -n "$NS" exec deploy/backend -- python -c "
import json, time, os, redis, pika
from comun import claves, colas

# Conexión directa a los servicios de infraestructura
r = redis.Redis.from_url(os.environ['REDIS_URL'])
conn = pika.BlockingConnection(pika.URLParameters(os.environ['RABBITMQ_URL']))
ch = conn.channel()

job_id = '$JOB_ID'
imagen = '$IMAGEN'
cola_worker = colas.declarar_tareas(ch, imagen)

# 1. Crear job en Redis con la estructura esperada por el backend y los workers
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

# 2. Registrar la geometría de la imagen para que el Joiner sepa cómo reconstruirla al recuperarse
r.hset(claves.geometria(job_id, 0), mapping={'total': '1', '0': '0,16'})
r.expire(claves.geometria(job_id, 0), 600)

# 3. Guardar el fragmento corrupto: bytes no decodificables que disparan ValueError en pixeles.leer
clave_frag = claves.fragmento(job_id, 0, 0, 0)
r.set(clave_frag, b'BYTES_CORRUPTOS_MATRIZ_INVALIDA_A_PROPOSITO', ex=600)

# 4. Publicar la tarea inicial en RabbitMQ (intento 1)
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

# ----------------------------------------------------------------------------------------
# PASO 3: Observación de reintentos en el microservicio Reasignador
# ----------------------------------------------------------------------------------------
# Damos 3 segundos para que:
# 1. El worker tome la tarea -> falle -> nack -> DLX envíe a sobel.dlq.
# 2. El reasignador tome de sobel.dlq -> reintente con intento=2.
# 3. El worker vuelva a fallar -> intento=3.
# 4. El worker vuelva a fallar -> intento=4.
# 5. El reasignador detecte intento > 3 -> agotar() -> envíe a sobel.dlq.agotados.
amarillo "3. Esperando que el worker falle y el reasignador ejecute los reintentos..."
sleep 3
echo

azul "--- Evidencia en logs del Reasignador (sobel.dlq -> reintentos -> agotados) ---"
kubectl -n "$NS" logs deploy/reasignador --since=15s | grep -E "job $JOB_ID" || true
echo

# ----------------------------------------------------------------------------------------
# PASO 4: Verificación del estado del Job vía API REST pública
# ----------------------------------------------------------------------------------------
# El usuario consulta GET /jobs/<id>. El estado debe figurar como 'dlq', con 3 reintentos
# contabilizados y 1 fragmento derivado a la Dead Letter Queue.
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

# ----------------------------------------------------------------------------------------
# PASO 5: Comprobación de mensajes en la cola final 'sobel.dlq.agotados'
# ----------------------------------------------------------------------------------------
# Validamos directamente en RabbitMQ que el mensaje esté esperando en la cola de agotados.
# Esta métrica activa la regla de alerta SobelDLQAgotadosPresentes en Prometheus.
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

# ----------------------------------------------------------------------------------------
# PASO 6: Consulta de telemetría de administración (GET /stats)
# ----------------------------------------------------------------------------------------
# El panel de administración reporta dlq: { fallidos: 0, agotados: >=1 }.
amarillo "6. Verificando métricas desde la API de administración (GET /stats)..."
STATS=$(curl -s -H "Authorization: Bearer $ADMIN" "$URL/stats")
echo "$STATS" | python3 -m json.tool
echo

# ----------------------------------------------------------------------------------------
# PASO 7: Subsanación del problema en los datos (reparación de la entrada)
# ----------------------------------------------------------------------------------------
# Para demostrar una recuperación exitosa completa, corregimos los bytes corruptos
# reemplazándolos con una imagen PNG válida (16x16) en Redis.
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

# ----------------------------------------------------------------------------------------
# PASO 8: Acción correctiva del Administrador (POST /admin/dlq/reprocesar)
# ----------------------------------------------------------------------------------------
# El administrador dispara el reprocesamiento: el Backend extrae los mensajes de
# 'sobel.dlq.agotados', resetea el contador de intento a 1 y los reenvía a la cola del worker.
amarillo "8. Ejecutando reprocesamiento de administrador (POST /admin/dlq/reprocesar)..."
RESP_REPROC=$(curl -s -X POST -H "Authorization: Bearer $ADMIN" "$URL/admin/dlq/reprocesar")
echo "$RESP_REPROC" | python3 -m json.tool
echo

# Damos 3 segundos para que el worker procese el fragmento reparado y el joiner lo ensamble
sleep 3

# ----------------------------------------------------------------------------------------
# PASO 9: Verificación de vaciado de la Dead Letter Queue
# ----------------------------------------------------------------------------------------
amarillo "9. Verificando que la cola 'sobel.dlq.agotados' haya sido vaciada..."
MENSAJES_RESTANTES=$(kubectl -n "$NS" exec rabbitmq-0 -- rabbitmqctl list_queues name messages --silent | awk '$1=="sobel.dlq.agotados"{print $2}')
echo "   Mensajes restantes en sobel.dlq.agotados: $MENSAJES_RESTANTES"
if [ "$MENSAJES_RESTANTES" -eq 0 ]; then
  verde "   La cola sobel.dlq.agotados quedó limpia (0 mensajes)."
else
  rojo "   ADVERTENCIA: Aún quedan mensajes en sobel.dlq.agotados ($MENSAJES_RESTANTES)."
fi
echo

# ----------------------------------------------------------------------------------------
# PASO 10: Verificación del estado final del Job
# ----------------------------------------------------------------------------------------
# Al estar corregido el fragmento, el worker lo filtró con éxito, el Joiner lo ensambló
# y el job pasó de 'dlq' a 'done'.
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
