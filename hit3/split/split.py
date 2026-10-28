"""Split: toma cada job de sobel.split, parte sus archivos en fragmentos y publica una tarea por fragmento."""

# -------------------------------------------------------------------------------------------------
# 1. IMPORTACIONES
# -------------------------------------------------------------------------------------------------
import json      # Para serializar y deserializar datos JSON (mensajes y datos en Redis)
import logging   # Para emitir logs informativos, advertencias y errores en consola
import signal    # Para interceptar señales del sistema operativo (como SIGTERM de Kubernetes)
import sys       # Para operaciones del intérprete, como sys.exit para apagado limpio
import time      # Para medir marcas de tiempo (timestamps) y calcular demoras en milisegundos

# Módulos comunes del proyecto (carpeta hit3/comun/)
from comun import claves, colas, conexion, metricas, pixeles, registro, salud


# -------------------------------------------------------------------------------------------------
# 2. CONSTANTES
# -------------------------------------------------------------------------------------------------

# Operaciones que analizan píxeles vecinos con una matriz de convolución 3x3.
# Necesitan 1 fila extra de borde (halo) arriba y abajo para calcular los bordes correctamente.
OPERACIONES_DE_VECINDAD = {"sobel", "blur"}


def ahora_en_ms():
    """Devuelve la marca de tiempo actual en milisegundos enteros (ej: 1761055391000)."""
    return int(time.time() * 1000)


def claves_de_entrada(job_id, archivos):
    """Devuelve la lista de nombres de claves en Redis donde el backend guardó las imágenes originales.
    Ejemplo para 2 archivos: ['job:j1:entrada:0', 'job:j1:entrada:1']
    """
    return [claves.entrada(job_id, archivo) for archivo in range(archivos)]


# -------------------------------------------------------------------------------------------------
# 3. MANEJO DE ARCHIVOS INVÁLIDOS Y CAMBIOS DE ESTADO
# -------------------------------------------------------------------------------------------------
def fallar_por_archivo_invalido(redis, job_id, archivos, mensaje, ahora):
    """Maneja el caso donde el usuario subió un archivo que no es una imagen válida (ej: texto plano o corrupto).
    
    ¿Qué hace?:
    1. Marca el estado del job como 'failed' y guarda el mensaje de error en Redis.
    2. Registra el trabajo en la lista global de 'terminados' para las métricas y el endpoint /stats.
    3. Borra las imágenes originales de Redis para no desperdiciar memoria RAM.
    4. Todo se ejecuta en un 'pipeline' (transacción atómica) para garantizar consistencia.
    """
    clave_job = claves.job(job_id)
    error = {"tipo": "ARCHIVO_INVALIDO", "mensaje": mensaje}
    total_ms = ahora - int(redis.hget(clave_job, "creado"))
    with redis.pipeline() as transaccion:
        transaccion.hset(clave_job, mapping={"estado": "failed", "error": json.dumps(error), "terminado": ahora})
        transaccion.lpush(claves.TERMINADOS, f"failed {total_ms}")
        transaccion.ltrim(claves.TERMINADOS, 0, 999)  # Mantiene solo los últimos 1000 trabajos
        transaccion.delete(*claves_de_entrada(job_id, archivos))
        transaccion.execute()


def pasar_a_processing(redis, job_id, t_split_ms):
    """Anota cuánto tardó el split ('t_split_ms') y pasa el job a 'processing' solo si sigue en 'queued'.
    
    ¿Por qué?: En jobs pequeños, los workers y el joiner son tan rápidos que pueden haberlo
    completado ('done') antes de que termine esta escritura. Con WATCH/transaction evitamos
    pisar un estado final 'done'.
    """
    clave_job = claves.job(job_id)

    def escribir(transaccion):
        en_cola = transaccion.hget(clave_job, "estado") == b"queued"
        transaccion.multi()
        transaccion.hset(clave_job, "t_split_ms", t_split_ms)
        if en_cola:
            transaccion.hset(clave_job, "estado", "processing")

    redis.transaction(escribir, clave_job)


# -------------------------------------------------------------------------------------------------
# 4. CÁLCULO DINÁMICO DE PARTES (Mejora solicitada en devolución)
# -------------------------------------------------------------------------------------------------
def calcular_partes(matriz, redis, archivos=1, canal=None, cola_tareas=None, partes_fijas=0):
    """Calcula dinámicamente en cuántas partes horizontales cortar la imagen.
    
    Evalúa 4 variables en simultáneo:
    1. Dimensiones y peso en memoria RAM de la imagen (matriz NumPy).
    2. Workers activos actualmente en el clúster (registrados en Redis bajo 'worker:*').
    3. Cantidad de archivos en el pedido (para evitar avalanchas de fragmentos).
    4. Mensajes acumulados en RabbitMQ (si la cola ya está saturada, no sobre-fracciona).

    Si 'partes_fijas' es > 0, respeta ese valor fijo (útil para tests unitarios).
    """
    # Si viene un override fijo explícito (ej: pruebas unitarias con partes=2), lo respetamos
    if partes_fijas and partes_fijas > 0:
        return partes_fijas

    alto, ancho = matriz.shape[:2]
    tamano_mb = matriz.nbytes / (1024 * 1024)

    # 1. Averiguar cuántos workers vivos tenemos en Redis
    try:
        workers_activos = len(list(redis.scan_iter(match="worker:*", count=100)))
    except Exception:
        workers_activos = 2  # Valor de respaldo seguro
    workers_activos = max(workers_activos, 1)

    # 2. Averiguar si la cola de RabbitMQ tiene mensajes en espera
    mensajes_en_cola = 0
    if canal is not None and cola_tareas is not None:
        try:
            # Declaración pasiva: solo consulta el estado de la cola sin modificarla
            estado_cola = canal.queue_declare(queue=cola_tareas, passive=True)
            mensajes_en_cola = getattr(estado_cola.method, "message_count", 0)
        except Exception:
            mensajes_en_cola = 0

    # 3. Lógica de corte adaptativo:
    # CASO A: Imagen pequeña (< 0.5 MB o menos de 300 filas de alto)
    # No vale la pena fraccionar mucho: 1 o 2 partes evitan overhead de red y mensajes.
    if tamano_mb < 0.5 or alto < 300:
        return 1 if alto < 150 else 2

    # CASO B: Imagen estándar / mediana (0.5 MB a 8 MB)
    # Se ajusta proporcionalmente a los workers vivos (ej. 4 a 8 partes).
    if tamano_mb < 8.0:
        base = max(workers_activos, 2)
        if mensajes_en_cola > 30:  # Si la cola está llena, limita el corte a 4
            return min(base, 4)
        return min(base, 8)

    # CASO C: Imagen grande (8 MB a 40 MB)
    # Busca que ninguna franja supere los 300 píxeles de alto para no sobrecargar CPU/RAM.
    if tamano_mb < 40.0:
        partes_por_alto = max(alto // 300, 4)
        base = max(workers_activos * 2, partes_por_alto)
        if mensajes_en_cola > 50:
            return min(base, 8)
        return min(base, 16)

    # CASO D: Imagen gigante (40 MB a 100 MB)
    # ¡Objetivo crítico!: Evitar OOMKilled en los workers generando franjas delgadas (máx 32 partes).
    partes_por_alto = max(alto // 250, 8)
    factor_archivos = max(archivos, 1)
    max_partes = max(32 // factor_archivos, 8)
    return int(min(max(partes_por_alto, workers_activos * 2), max_partes))


# -------------------------------------------------------------------------------------------------
# 5. MANEJADOR Y CONSUMO DE MENSAJES DE RABBITMQ
# -------------------------------------------------------------------------------------------------
def manejador(redis, partes=0, ttl=claves.TTL_POR_DEFECTO, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.split")):
    """Crea la función callback que se ejecutará cada vez que llegue un mensaje a la cola 'sobel.split'."""

    def procesar(canal, cuerpo):
        # Envuelve la ejecución: si ocurre una excepción no capturada, la registra en Prometheus
        # y rechaza el mensaje para que RabbitMQ lo envíe a la DLQ.
        with metricas.fallos("split", "split"):
            repartir(canal, cuerpo)

    def repartir(canal, cuerpo):
        inicio = ahora_ms()
        
        # 1. Leer el ID del trabajo desde el cuerpo del mensaje JSON
        job_id = json.loads(cuerpo)["job_id"]
        clave_job = claves.job(job_id)
        estado = redis.hget(clave_job, "estado")
        
        # Si el job ya no existe en Redis (expiró por TTL), se descarta
        if estado is None:
            log.warning("job %s no existe (venció o el backend no lo terminó de crear), se descarta", job_id)
            return
            
        # Si ya no está en 'queued' (ej: reentrega tras un corte de red), no volvemos a encolar
        if estado != b"queued":
            log.info("job %s ya está en %s, no se reparte de nuevo", job_id, estado.decode())
            return
            
        # 2. Leer las operaciones a realizar (ej: [{"operacion": "sobel", "imagen": "..."}])
        operaciones = json.loads(redis.hget(clave_job, "operaciones"))
        
        # Contar cuántas operaciones necesitan halo de borde (1 fila extra arriba y abajo por cada filtro 3x3)
        filas_de_borde = sum(paso["operacion"] in OPERACIONES_DE_VECINDAD for paso in operaciones)
        nombre_cola = f"{colas.PREFIJO_TAREAS}{operaciones[0]['imagen']}"
        
        tareas = []
        archivos = int(redis.hget(clave_job, "archivos"))
        
        # 3. Procesar cada imagen del pedido
        for archivo in range(archivos):
            try:
                # Descargar los bytes de Redis y decodificarlos en matriz NumPy RGB
                matriz = pixeles.leer_rgb(redis.get(claves.entrada(job_id, archivo)))
            except ValueError:
                # Si el archivo está corrupto, marcamos el job como failed y salimos
                mensaje = f"el archivo {archivo} no se pudo decodificar"
                fallar_por_archivo_invalido(redis, job_id, archivos, mensaje, ahora_ms())
                log.warning("job %s: %s, queda en failed", job_id, mensaje)
                metricas.fallo("split", "split")
                metricas.ERRORES.labels("split", "ARCHIVO_INVALIDO").inc()
                return

            # 4. Calcular cantidad de partes de forma adaptativa
            n_partes = calcular_partes(matriz, redis, archivos=archivos, canal=canal, cola_tareas=nombre_cola,
                                       partes_fijas=partes)
                                       
            # 5. Cortar la matriz en franjas horizontales con sus filas de halo añadidas
            fragmentos = pixeles.dividir(matriz, n_partes, filas_de_borde)
            
            # 6. Guardar cada franja en Redis como PNG con vencimiento (TTL)
            for f, (franja, _, _) in enumerate(fragmentos):
                redis.set(claves.fragmento(job_id, archivo, f, 0), pixeles.a_png(franja), ex=ttl)
                tareas.append({
                    "job_id": job_id,
                    "archivo": archivo,
                    "fragmento": f,
                    "paso": 0,
                    "operaciones": operaciones,
                    "intento": 1
                })
                
            # 7. Guardar la "geometría" en Redis: información de filas y bordes para que el Joiner pueda rearmarla
            clave_geometria = claves.geometria(job_id, archivo)
            with redis.pipeline() as transaccion:
                transaccion.hset(clave_geometria, mapping=pixeles.hash_de_geometria(fragmentos))
                transaccion.expire(clave_geometria, ttl)
                transaccion.execute()

        # 8. Anotar el total de tareas en Redis para que frontend y joiner sepan cuánto esperar
        redis.hset(clave_job, "fragmentos_total", len(tareas))
        
        # 9. Declarar la cola de tareas en RabbitMQ y publicar un mensaje por cada fragmento
        cola = colas.declarar_tareas(canal, operaciones[0]["imagen"])
        for tarea in tareas:
            colas.publicar(canal, cola, {**tarea, "encolado": ahora_ms()})
            
        t_split_ms = ahora_ms() - inicio
        pasar_a_processing(redis, job_id, t_split_ms)
        
        # 10. Borrar las imágenes originales de Redis: ya no hacen falta porque están fragmentadas
        redis.delete(*claves_de_entrada(job_id, archivos))
        log.info("job %s: %d fragmentos a %s en %d ms", job_id, len(tareas), cola, t_split_ms)
        metricas.trabajo("split", "split", t_split_ms / 1000)

    return procesar


# -------------------------------------------------------------------------------------------------
# 6. PUNTO DE ENTRADA PRINCIPAL (MAIN)
# -------------------------------------------------------------------------------------------------
def main():
    log, _ = registro.configurar("split")

    # Manejador de señal SIGTERM para apagado elegante en Kubernetes
    def apagar(senal, _frame):
        log.info("%s: el split termina; si había un job en curso, RabbitMQ lo reentrega",
                 signal.Signals(senal).name)
        sys.exit(0)

    signal.signal(signal.SIGTERM, apagar)

    # Lectura de variables de entorno (con valores por defecto seguros)
    # SOBEL_PARTES: 0 activa el cálculo dinámico adaptativo
    partes = conexion.entero_de_entorno("SOBEL_PARTES", 0, 0)
    ttl = conexion.entero_de_entorno("SOBEL_TTL", claves.TTL_POR_DEFECTO, 1)
    puerto = conexion.entero_de_entorno("SOBEL_PUERTO", 8080, 1)

    redis = conexion.cliente_redis()

    # Iniciar servidor HTTP en segundo plano para endpoints de salud (/health) y métricas (/metrics)
    salud.servir("split", puerto, conexion.estado, redis=redis)
    log.info("split: /health en el puerto %d", puerto)

    # Conectar a Redis con reintentos y retroceso exponencial (backoff)
    conexion.con_backoff(redis.ping, "redis", log=log)

    if partes > 0:
        log.info("split consumiendo %s, %d partes fijas por archivo", colas.SPLIT, partes)
    else:
        log.info("split consumiendo %s, partes calculadas dinámicamente", colas.SPLIT)

    # Bucle principal de consumo de mensajes de la cola 'sobel.split' de RabbitMQ
    conexion.consumir(colas.SPLIT, manejador(redis, partes, ttl, log=log), al_conectar=colas.declarar,
                      cliente_redis=redis, servicio="split", log=log)


if __name__ == "__main__":
    main()

