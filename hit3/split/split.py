"""Split: toma cada job de sobel.split, parte sus archivos en fragmentos y publica una tarea por fragmento."""

# -------------------------------------------------------------------------------------------------
# 1. IMPORTACIONES
# -------------------------------------------------------------------------------------------------
import json      # Para serializar y deserializar datos JSON (mensajes y datos en Redis)
import logging   # Para emitir logs informativos, advertencias y errores en consola
import math      # Para redondear hacia arriba la cantidad de fragmentos
import signal    # Para interceptar señales del sistema operativo (como SIGTERM de Kubernetes)
import sys       # Para operaciones del intérprete, como sys.exit para apagado limpio
import time      # Para medir marcas de tiempo (timestamps) y calcular demoras en milisegundos

# Módulos comunes del proyecto (carpeta hit3/comun/)
from comun import almacen as punteros
from comun import claves, colas, conexion, dinamico, metricas, pixeles, registro, salud


# -------------------------------------------------------------------------------------------------
# 2. CONSTANTES
# -------------------------------------------------------------------------------------------------

# Operaciones que analizan píxeles vecinos con una matriz de convolución 3x3.
# Necesitan 1 fila extra de borde (halo) arriba y abajo para calcular los bordes correctamente.
OPERACIONES_DE_VECINDAD = {"sobel", "blur"}

# Una franja de menos filas que esto es casi todo halo y overhead: el split no corta más fino.
FILAS_MIN = 64


def ahora_en_ms():
    """Devuelve la marca de tiempo actual en milisegundos enteros (ej: 1761055391000)."""
    return int(time.time() * 1000)


def claves_de_entrada(job_id, archivos):
    """Devuelve la lista de claves de Redis con el puntero a cada imagen original (que el backend subió al bucket).
    Ejemplo para 2 archivos: ['job:j1:entrada:0', 'job:j1:entrada:1']
    """
    return [claves.entrada(job_id, archivo) for archivo in range(archivos)]


# -------------------------------------------------------------------------------------------------
# 3. MANEJO DE ARCHIVOS INVÁLIDOS Y CAMBIOS DE ESTADO
# -------------------------------------------------------------------------------------------------
def fallar_por_archivo_invalido(redis, almacen, job_id, archivos, mensaje, ahora):
    """Maneja el caso donde el usuario subió un archivo que no es una imagen válida (ej: texto plano o corrupto).
    
    ¿Qué hace?:
    1. Marca el estado del job como 'failed' y guarda el mensaje de error en Redis.
    2. Registra el trabajo en la lista global de 'terminados' para las métricas y el endpoint /stats.
    3. Borra las imágenes originales del bucket (y sus punteros de Redis): nadie las va a procesar.
    Los pasos 1 y 2 van en un 'pipeline' (transacción atómica) para garantizar consistencia.
    """
    clave_job = claves.job(job_id)
    error = {"tipo": "ARCHIVO_INVALIDO", "mensaje": mensaje}
    total_ms = ahora - int(redis.hget(clave_job, "creado"))
    with redis.pipeline() as transaccion:
        transaccion.hset(clave_job, mapping={"estado": "failed", "error": json.dumps(error), "terminado": ahora})
        transaccion.lpush(claves.TERMINADOS, f"failed {total_ms}")
        transaccion.ltrim(claves.TERMINADOS, 0, 999)  # Mantiene solo los últimos 1000 trabajos
        transaccion.execute()
    punteros.borrar(redis, almacen, *claves_de_entrada(job_id, archivos))


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
def calcular_partes(matriz, redis, canal=None, cola_tareas=None, partes_fijas=0):
    """En cuántas franjas horizontales cortar una imagen: según su TAMAÑO, no según cuántos workers haya.

    1. Tamaño: una franja cada `mb_por_fragmento` MB de píxeles. Una imagen chica sale en un solo fragmento (partirla
       sólo suma viajes al bucket y mensajes); una de 100 MB, en ~13 de 8 MB.
    2. Cola: si la cola de tareas ya pasa `umbral_cola` mensajes, los workers no dan abasto y más fragmentos sólo
       suman overhead: se corta en la mitad (franjas del doble de tamaño).
    3. Topes: `max_partes` por imagen, y ninguna franja de menos de FILAS_MIN filas.

    No mira los workers activos: con un tamaño de fragmento fijo, la cola crece con el trabajo pendiente (bytes) y
    no con cuántos pods hay, y es esa cola la que dice cuándo hacen falta más workers. Las tres perillas se cambian
    en caliente con PUT /admin/config (comun/dinamico.py). `partes_fijas` > 0 (SOBEL_PARTES) pisa todo.
    """
    if partes_fijas and partes_fijas > 0:
        return partes_fijas

    cfg = dinamico.leer(redis)
    alto = matriz.shape[0]
    tamano_mb = matriz.nbytes / (1024 * 1024)
    partes = math.ceil(tamano_mb / cfg["mb_por_fragmento"])

    if canal is not None and cola_tareas is not None and partes > 1:
        try:  # declaración pasiva: sólo consulta cuántos mensajes hay, sin modificar la cola
            en_cola = canal.queue_declare(queue=cola_tareas, passive=True).method.message_count
        except Exception:
            en_cola = 0
        if en_cola > cfg["umbral_cola"]:
            partes = math.ceil(partes / 2)

    return max(1, min(partes, cfg["max_partes"], alto // FILAS_MIN))


# -------------------------------------------------------------------------------------------------
# 5. MANEJADOR Y CONSUMO DE MENSAJES DE RABBITMQ
# -------------------------------------------------------------------------------------------------
def manejador(redis, partes=0, ttl=claves.TTL_POR_DEFECTO, ahora_ms=ahora_en_ms, log=logging.getLogger("tp3.split"),
              *, almacen):
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
        # Se declara antes de medirla: si no existiera, el queue_declare pasivo de calcular_partes cerraría el canal.
        cola = colas.declarar_tareas(canal, operaciones[0]["imagen"])
        
        tareas = []
        archivos = int(redis.hget(clave_job, "archivos"))
        
        # 3. Procesar cada imagen del pedido
        for archivo in range(archivos):
            try:
                # Seguir el puntero de Redis, bajar los bytes del bucket y decodificarlos en matriz NumPy RGB
                matriz = pixeles.leer_rgb(punteros.leer(redis, almacen, claves.entrada(job_id, archivo)))
            except ValueError:
                # Si el archivo está corrupto, marcamos el job como failed y salimos
                mensaje = f"el archivo {archivo} no se pudo decodificar"
                fallar_por_archivo_invalido(redis, almacen, job_id, archivos, mensaje, ahora_ms())
                log.warning("job %s: %s, queda en failed", job_id, mensaje)
                metricas.fallo("split", "split")
                metricas.ERRORES.labels("split", "ARCHIVO_INVALIDO").inc()
                return

            # 4. Calcular cantidad de partes de forma adaptativa
            n_partes = calcular_partes(matriz, redis, canal=canal, cola_tareas=cola, partes_fijas=partes)
                                       
            # 5. Cortar la matriz en franjas horizontales con sus filas de halo añadidas
            fragmentos = pixeles.dividir(matriz, n_partes, filas_de_borde)
            nivel_png = dinamico.leer(redis)["png_nivel"]

            # 6. Subir cada franja al bucket como PNG (en paralelo) y dejar su puntero en Redis con vencimiento (TTL)
            punteros.escribir_varios(redis, almacen, [(claves.fragmento(job_id, archivo, f, 0),
                                                       pixeles.a_png(franja, nivel=nivel_png))
                                                      for f, (franja, _, _) in enumerate(fragmentos)], ttl)
            for f in range(len(fragmentos)):
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
        for tarea in tareas:
            colas.publicar(canal, cola, {**tarea, "encolado": ahora_ms()})
            
        t_split_ms = ahora_ms() - inicio
        pasar_a_processing(redis, job_id, t_split_ms)
        
        # 10. Borrar las imágenes originales (bucket y punteros): ya no hacen falta porque están fragmentadas
        punteros.borrar(redis, almacen, *claves_de_entrada(job_id, archivos))
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
    almacen = punteros.desde_entorno()

    # Iniciar servidor HTTP en segundo plano para endpoints de salud (/health) y métricas (/metrics)
    salud.servir("split", puerto, conexion.estado, redis=redis)
    log.info("split: /health en el puerto %d", puerto)

    # Conectar a Redis con reintentos y retroceso exponencial (backoff)
    conexion.con_backoff(redis.ping, "redis", log=log)
    conexion.con_backoff(almacen.asegurar_bucket, "almacen", log=log)

    if partes > 0:
        log.info("split consumiendo %s, %d partes fijas por archivo", colas.SPLIT, partes)
    else:
        log.info("split consumiendo %s, partes calculadas dinámicamente", colas.SPLIT)

    # Bucle principal de consumo de mensajes de la cola 'sobel.split' de RabbitMQ
    conexion.consumir(colas.SPLIT, manejador(redis, partes, ttl, log=log, almacen=almacen), al_conectar=colas.declarar,
                      cliente_redis=redis, servicio="split", log=log)


if __name__ == "__main__":
    main()

