# Pruebas de Carga con k6 — Matriz R5 (Hit #3)

Este directorio contiene la suite de pruebas de carga automatizada con **k6** para evaluar el rendimiento, la escalabilidad y la tolerancia a fallos del sistema distribuido asincrónico Sobel, según los requisitos de **R5** y el contrato definido en `hit3/contrato.md`.

---

## 1. Dimensiones de la Matriz R5

Las pruebas exploran el espacio de estados cruzando cinco dimensiones clave:

| Dimensión | Significado | Valores Evaluados |
|---|---|---|
| **$VA$** | Cantidad de archivos por job | `1`, `10`, `50` archivos |
| **$VB$** | Cantidad de operaciones encadenadas | `1` (`sobel`), `2` (`grises` → `sobel`), `3` (`grises` → `blur` → `sobel`) |
| **$VC$** | Usuarios virtuales concurrentes (VUs) | `1`, `10`, `50` usuarios con token JWT individual y bots sin token |
| **$V1$** | Tamaño de las imágenes | `1kb`, `1mb`, `10mb`, `50mb` |
| **$V3$** | Cantidad de workers activos | Parámetro del cluster (ej: `2`, `4`, `8` réplicas en pods o VMs) |

---

## 2. Definición Estricta de los SLOs

De acuerdo con la **Sección 8 del Contrato**, una celda de la matriz **CUMPLE** si y solo si se satisfacen simultáneamente tres condiciones:

1. **SLO 1 (Respuesta Inmediata):**  
   $$p_{95}(\text{POST } /jobs) \le 1000\text{ ms}$$  
   El backend debe encolar el pedido y responder `202 Accepted` de forma inmediata sin bloquearse por la carga encolada.

2. **SLO 2 (Procesamiento Distribuido Total):**  
   $$p_{95}(\text{tiempos\_s.total}) \le 30\text{ s} \quad (\text{para pedidos } \le 10\text{ MB en total})$$  
   $$p_{95}(\text{tiempos\_s.total}) \le 120\text{ s} \quad (\text{para pedidos } > 10\text{ MB})$$  
   Mide el ciclo de vida completo: desde la aceptación en `POST /jobs`, pasando por el split, el procesamiento distribuido en los workers, la resolución en DLQ si aplica, hasta el ensamblado final en el joiner (`done`).

3. **SLO 3 (Tasa de Errores no Provocados):**  
   $$\text{Tasa de Error} < 1\%$$  
   Porcentaje de respuestas `5xx`, o jobs que terminen en estado `failed` o `dlq` sin haber sido provocados intencionalmente.

---

## 3. Casos Inválidos Inyectados Durante la Carga

La suite no solo evalúa peticiones felices, sino que introduce un porcentaje controlado de tráfico anómalo para verificar la robustez del backend:

* **Bots sin autenticación (`401 NO_AUTORIZADO`):** Peticiones sin cabecera `Authorization` o con tokens vencidos. Deben ser rechazadas en el borde sin tocar RabbitMQ ni Redis.
* **Operaciones no soportadas (`422 OPERACION_NO_SOPORTADA`):** Peticiones solicitando operaciones inexistentes (ej: `rotar`). Se validan antes de encolar.
* **Archivos corruptos (`422 ARCHIVO_INVALIDO`):** Archivos con extensión válida pero bytes truncados o sin cabecera de imagen. Se detectan en la validación inicial.
* **Límite de tasa (`429 DEMASIADOS_PEDIDOS`):** Cuando un mismo usuario supera los 120 pedidos por minuto, el backend responde `429` con cabecera `Retry-After`.
* **Caída de worker (Tolerancia a fallos):** Al matar un worker durante una corrida masiva, los mensajes deben reencolarse vía RabbitMQ DLX hacia la cola `sobel.dlq`, ser reasignados y permitir que el job termine exitosamente.

---

## 4. Estructura de Archivos

```
hit3/carga/
├── correr_matriz.sh     # Runner automatizado en Bash (soporta k6 local o Docker)
├── generar_datos.py     # Generador de imágenes PNG calibradas y tokens JWT
├── matriz_r5.js         # Script k6 central configurable para cualquier celda
├── smoke_test.js        # Test de humo para validación rápida del contrato
├── README.md            # Documentación de uso y análisis
├── datos/               # Imágenes sintéticas y tokens (ignorado en git)
└── resultados/          # Informes en JSON y Markdown (ignorado en git)
```

---

## 5. Instrucciones de Uso

### Requisitos Previos
* **Python 3:** Con `pillow` y `numpy` para generar las imágenes sintéticas (`pip install pillow numpy`).
* **k6 o Docker:** El script detecta automáticamente si `k6` está instalado en el sistema operativo; de lo contrario, utiliza la imagen oficial `grafana/k6:latest` mediante Docker de forma transparente.

### A. Test de Humo Rápido
Verifica la conectividad, las respuestas de error y el ciclo de un job simple:
```bash
./correr_matriz.sh --smoke
```

### B. Ejecutar una Celda Específica
Permite probar una combinación puntual definiendo $VA$, $VB$, $VC$ y $V1$:
```bash
# Ejemplo: 10 archivos, 2 operaciones (grises -> sobel), 10 usuarios virtuales, imágenes de 1 MB
./correr_matriz.sh --celda 10 2 10 1mb

# Ejemplo: 1 archivo grande de 10 MB con 1 usuario
./correr_matriz.sh --celda 1 1 1 10mb
```

### C. Ejecutar la Corrida Representativa de la Matriz R5
Ejecuta secuencialmente las celdas de prueba y genera un informe Markdown consolidado:
```bash
./correr_matriz.sh --matriz
```

### Variables de Entorno de Configuración
Puedes sobreescribir los destinos y parámetros al ejecutar:
```bash
TARGET_URL="http://sobel.local" V3=4 DURATION="45s" ./correr_matriz.sh --matriz
```

* `TARGET_URL`: Dirección base del backend (por defecto `http://localhost:8080`).
* `V3`: Cantidad de workers activos para documentar en los metadatos (por defecto `2`).
* `DURATION`: Tiempo de carga sostenida por celda (por defecto `30s`).
* `RAMP_UP`: Tiempo de incremento de carga (por defecto `5s`).

---

## 6. Monitoreo Simultáneo en Grafana (Hit #4)

Mientras k6 ejecuta las pruebas, es fundamental observar en vivo el dashboard de Grafana configurado en `hit4/dashboards/sobel-dashboard.json`:

1. **Throughput de mensajes:** Observar la correlación entre peticiones en k6 y la tasa de mensajes por segundo en RabbitMQ.
2. **Cola saturada:** En celdas con $VC=50$ o $VA=50$, verificar cómo la cola `sobel.tareas.*` acumula mensajes y dispara la alerta `SobelColaSaturada` cuando supera los 50 fragmentos.
3. **Consumo de recursos:** Validar que los pods de workers escalen su consumo de CPU hacia el límite fijado en los manifiestos (`500m`).
4. **DLQ y reintentos:** En caso de fallas simuladas (ej: `docker kill` a un worker), observar el panel **Mensajes en DLQ** y verificar que el reasignador devuelva los fragmentos sin pérdida de datos.
