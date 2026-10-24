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
| **$V1$** | Tamaño de las imágenes | `1kb`, `1mb`, `10mb`, `50mb`, `100mb` |
| **$V3$** | Cantidad de workers activos | `--completa` escala el Deployment a cada valor de `V3_VALORES` (por defecto `2` y `4`) |

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
* **Caída de worker (Tolerancia a fallos):** La última celda de `--matriz` borra un pod del worker a los 15 s de carga; los mensajes deben reencolarse vía RabbitMQ DLX hacia la cola `sobel.dlq`, ser reasignados y permitir que el job termine exitosamente.

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
└── resultados/<fecha>/  # JSON de cada celda e informe.md de la corrida (ignorado en git)
```

---

## 5. Instrucciones de Uso

### Requisitos Previos
* **Python 3** con `pillow` y `numpy` (`pip install pillow numpy`) para generar los datos.
* **`SOBEL_SECRETO`** exportado, o el `.env.secrets` que deja `hit3/k8s/crear_secretos.sh`: firma los tokens. Sin él,
  `generar_datos.py` corta (con un secreto inventado todo daría 401).
* **k6 o Docker:** si no hay `k6` instalado, usa la imagen `grafana/k6:1.8.1`.
* **`kubectl`** para `--completa` (escala el worker) y para la celda con un worker que muere.

### Modos
```bash
./correr_matriz.sh --smoke                 # contrato: 401, 422, 202 y un job hasta done
./correr_matriz.sh --celda 10 2 10 1mb     # una celda: VA VB VC V1, con los workers que haya
./correr_matriz.sh --matriz                # 8 celdas representativas; la última mata un worker a mitad de la carga
./correr_matriz.sh --completa              # VA × VB × VC × V1_VALORES para cada V3_VALORES (larga: más de 1 h)
./correr_matriz.sh --v1                    # barrido de tamaño 1 KB → 100 MB con VA=1, VB=1, VC=1
```

Cada corrida deja sus JSON y un `informe.md` con los tres SLOs en `resultados/<fecha>/`. V3 es la cantidad de
réplicas listas del Deployment `sobel-worker` al empezar cada celda. Las celdas cuyo pedido pasa de 200 MiB
(por ejemplo, 50 archivos de 10 MB) se saltean: el contrato las rechaza con `413`.

### Variables de Entorno
* `TARGET_URL`: URL del ingress (por defecto `http://localhost`).
* `DURATION` / `RAMP_UP`: meseta y rampa de cada celda (por defecto `30s` y `5s`); la bajada es de `5s`.
* `V3_VALORES` / `V1_VALORES`: valores de `--completa` (por defecto `"2 4"` y `"1mb 10mb"`).
* `SOBEL_NAMESPACE` / `SOBEL_DEPLOY_WORKER`: dónde está el worker (por defecto `sobel` y `sobel-worker`).
* `MAX_POLL_TIMEOUT_S`: espera máxima de un job; por defecto, el umbral del SLO 2 más 30 s.

---

## 6. Monitoreo Simultáneo en Grafana (Hit #4)

Mientras k6 ejecuta las pruebas, es fundamental observar en vivo el dashboard de Grafana configurado en `hit4/dashboards/sobel-dashboard.json`:

1. **Throughput de mensajes:** Observar la correlación entre peticiones en k6 y la tasa de mensajes por segundo en RabbitMQ.
2. **Cola saturada:** En celdas con $VC=50$ o $VA=50$, verificar cómo la cola `sobel.tareas.*` acumula mensajes y dispara la alerta `SobelColaSaturada` cuando supera los 50 fragmentos.
3. **Consumo de recursos:** Validar que los pods de workers escalen su consumo de CPU hacia el límite fijado en los manifiestos (`1000m`).
4. **DLQ y reintentos:** En la celda que mata un worker, observar el panel **Mensajes en DLQ** y verificar que el reasignador devuelva los fragmentos sin pérdida de datos.
