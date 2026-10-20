# Contrato — Hit #3 (Sobel asincrónico)

La parte 1 es la API pública (R3): qué se manda, qué se responde, los errores y lo que no se devuelve.
La parte 2 es cómo se hablan los servicios por dentro: colas, mensajes, claves de Redis, métricas y
configuración. Cualquier cambio a este archivo se avisa en el grupo: los demás programan contra él.

```mermaid
flowchart LR
    C[cliente] -->|POST /jobs| B[backend]
    B -->|sobel.split| S[split]
    S -->|"sobel.tareas.#lt;imagen#gt;"| W[worker]
    W -->|"siguiente paso"| W
    W -->|"último paso"| F{{sobel.resultados<br/>fanout}}
    F --> J[joiner]
    F --> D[frontend / dashboard]
    W -.->|"crash, timeout, nack"| DLQ[(sobel.dlq)]
    DLQ --> R[reasignador]
    R -->|reintento| W
    R -.->|"agotó los reintentos"| A[(sobel.dlq.agotados)]
    B & S & W & J & R --- RD[(Redis)]
```

# Parte 1 — API pública

## 1. Autenticación y roles

Los servicios **no guardan sesión**: la identidad viaja en cada pedido y todo el estado vive en Redis y
RabbitMQ, así que da lo mismo qué réplica del backend atienda.

- **Cabecera:** `Authorization: Bearer <token>`. JWT HS256 con `sub` (usuario), `role` (`usuario` o `admin`)
  y `exp` (obligatorio). Lo emite `hit3/backend/emitir_token.py` con el secreto `SOBEL_SECRETO`, que vive en
  un Secret de Kubernetes.
- **Límite:** 120 pedidos por minuto por usuario (`SOBEL_PEDIDOS_POR_MINUTO`). El contador está en Redis, así que
  vale para todas las réplicas juntas. Pasado el límite, `429` con `Retry-After`.

| Ruta | Sin token | `usuario` | `admin` |
|---|---|---|---|
| `POST /jobs` | 401 | crea | crea |
| `GET /jobs` | 401 | los suyos | todos (o `?usuario=`) |
| `GET /jobs/{id}` y `GET /jobs/{id}/resultado/{n}` | 401 | el suyo; de otro, **404** | cualquiera |
| `GET /stats` | 401 | 403 | 200 |
| `POST /admin/dlq/reprocesar` | 401 | 403 | 200 |
| `GET /health` | 200 (pública) | 200 | 200 |

Un job de otro usuario da `404`, no `403`: así no se puede averiguar qué `job_id` existen.

## 2. Envío: `POST /jobs`

`multipart/form-data` con dos campos:

| Campo | Obligatorio | Reglas |
|---|---|---|
| `archivos` | sí | De 1 a 50 partes (se repite el campo). PNG o JPEG: se mira el contenido, no la extensión. Hasta 100 MiB y 100 megapíxeles cada uno; 200 MiB el pedido entero |
| `operaciones` | sí | JSON: lista de 1 a 3 pasos `{"operacion", "imagen"}` que se aplican en orden a cada archivo |

- **`imagen`** es la imagen de worker que resuelve ese paso, con tag o digest (nunca `latest`). Tiene que estar en
  `SOBEL_IMAGENES_PERMITIDAS`.
- **`operacion`** tiene que ser una de las que ofrece esa imagen. **Se detecta en el momento**, no está fija en el
  código: cada worker, al arrancar, registra en Redis su imagen y sus operaciones (§10). Hoy las imágenes de worker
  ofrecen `sobel`, `grises` y `blur`.
- Todo se valida **antes de encolar**. Un pedido inválido no llega a RabbitMQ.

```bash
curl -s -X POST http://<host>/jobs -H "Authorization: Bearer $TOKEN" \
  -F archivos=@gato.png -F archivos=@perro.jpg \
  -F 'operaciones=[{"operacion":"grises","imagen":"ghcr.io/chimera-sdypp/sobel-worker:1.0.0"},
                   {"operacion":"sobel","imagen":"ghcr.io/chimera-sdypp/sobel-worker:1.0.0"}]'
```

Respuesta inmediata, `202` con `Location: /jobs/<id>`. Todas las respuestas usan el sobre `{"codigo", "contenido"}`, como en el TP2:

```json
{"codigo": 202, "contenido": {"job_id": "3f6c2a9e-…", "estado": "queued", "url": "/jobs/3f6c2a9e-…",
  "archivos": 2, "creado": "2026-10-21T14:03:11Z"}}
```

## 3. Consulta: `GET /jobs/{id}`

```json
{"codigo": 200, "contenido": {
  "job_id": "3f6c2a9e-…", "usuario": "ana", "estado": "processing",
  "creado": "2026-10-21T14:03:11Z", "terminado": null,
  "archivos": 2,
  "operaciones": [{"operacion": "grises", "imagen": "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"},
                  {"operacion": "sobel",  "imagen": "ghcr.io/chimera-sdypp/sobel-worker:1.0.0"}],
  "fragmentos": {"total": 8, "completos": 5, "porcentaje": 62.5, "en_dlq": 0},
  "tiempos_s": {"split": 0.21, "cola": 0.04, "procesamiento": 0.37, "join": null, "total": null},
  "workers": ["sobel-worker-7d9f-x2k4", "sobel-worker-7d9f-q8n1"],
  "reintentos": 1,
  "resultados": [],
  "error": null}}
```

| Campo | Qué es |
|---|---|
| `estado` | Ver la tabla de abajo |
| `fragmentos` | `total` = suma de los fragmentos de todos los archivos. `completos` = los que ya pasaron por todas las operaciones. `en_dlq` = los que agotaron los reintentos y esperan que un admin los reprocese |
| `tiempos_s.split` | Lo que tardó el split en partir todos los archivos |
| `tiempos_s.cola` | Espera **promedio** de un fragmento en la cola, por paso |
| `tiempos_s.procesamiento` | Lo que tarda **en promedio** un worker en procesar un fragmento, por paso |
| `tiempos_s.join` | Lo que tardó el joiner en unir todos los archivos (`null` hasta `done`) |
| `tiempos_s.total` | De `creado` a `terminado` |
| `workers` | Los workers que procesaron al menos un fragmento |
| `reintentos` | Cuántas veces el reasignador volvió a encolar un fragmento sacado de la DLQ |
| `resultados` | Con `done`, una URL por archivo, en el orden en que se mandaron: `/jobs/{id}/resultado/0`, … |
| `error` | Con `failed` o `dlq`: `{"tipo", "mensaje"}` |

| Estado | Significa | Lo pone |
|---|---|---|
| `queued` | Aceptado, esperando al split | backend |
| `processing` | Partido; los fragmentos están en los workers | split (o el backend al reprocesar la DLQ) |
| `done` | Todos los archivos unidos; los resultados se pueden bajar | joiner |
| `failed` | Error que no se arregla reintentando: un archivo que pasó la validación pero no se puede decodificar | split |
| `dlq` | Al menos un fragmento agotó los reintentos. Un admin lo puede reprocesar y vuelve a `processing` | reasignador |

`done` y `failed` son finales: nadie los cambia después.

**Otras rutas del usuario**

- `GET /jobs/{id}/resultado/{n}`: la imagen `n` (desde 0) como `image/png`. `409 JOB_NO_TERMINADO` si todavía no está `done`.
- `GET /jobs?estado=done`: los últimos 100 jobs del usuario (`admin`: de todos), del más nuevo al más viejo, con
  `job_id`, `usuario`, `estado` y `creado`.

## 4. Admin

**`GET /stats`**

```json
{"codigo": 200, "contenido": {
  "jobs": {"queued": 0, "processing": 3, "done": 120, "failed": 2, "dlq": 1},
  "colas": {"sobel.split": 0, "sobel.tareas.ghcr.io/chimera-sdypp/sobel-worker:1.0.0": 14, "sobel.joiner": 2},
  "dlq": {"fallidos": 0, "agotados": 3},
  "workers_activos": 4,
  "latencia_s": {"p50": 1.8, "p95": 4.1, "p99": 7.9, "muestras": 123},
  "tasa_error": 0.024}}
```

- `jobs` cuenta los jobs vigentes (Redis los guarda 24 h).
- `colas` y `dlq` son los mensajes que hay ahora en cada cola. `fallidos` es `sobel.dlq` (el reasignador los
  está por reintentar); `agotados` es `sobel.dlq.agotados`.
- `latencia_s` es el `tiempos_s.total` de los últimos 1000 jobs terminados.
- `tasa_error` es la proporción de `failed` + `dlq` entre esos mismos jobs.

**`POST /admin/dlq/reprocesar`** vuelve a encolar todo lo que está en `sobel.dlq.agotados`, con `intento` en 1, y
pasa esos jobs a `processing`: `{"reprocesados": 3, "jobs": ["3f6c2a9e-…"]}`.

## 5. Health

`GET /health` es público y resume el estado de **cada servicio**:

```json
{"codigo": 200, "contenido": {"backend": "ok", "redis": "ok", "rabbitmq": "ok", "split": "ok",
  "joiner": "ok", "reasignador": "ok", "frontend": "ok", "workers": "ok"}}
```

Cada valor es `ok` o `caido`. `workers` está `ok` si hay al menos un worker registrado. Devuelve `503` sólo
si el backend no puede atender (Redis o RabbitMQ caídos); un `caido` en otro servicio es informativo y sigue en `200`.

## 6. Errores

`contenido` es `{"error": {"tipo", "mensaje", "detalles"}}`. **Los clientes deciden por `tipo`**; `mensaje` es para
personas. `detalles` es una lista: vacía, salvo en `PAYLOAD_INVALIDO` y `ARCHIVO_INVALIDO`, donde cada elemento es `{"campo", "problema"}`.

```json
{"codigo": 422, "contenido": {"error": {"tipo": "OPERACION_NO_SOPORTADA",
  "mensaje": "la imagen ghcr.io/chimera-sdypp/sobel-worker:1.0.0 no ofrece la operación 'rotar'", "detalles": []}}}
```

| Código | `tipo` | Cuándo ocurre | `mensaje` de ejemplo |
|---|---|---|---|
| **400** | `CUERPO_INVALIDO` | El multipart está mal formado o cortado | `"el cuerpo multipart está mal formado"` |
| **401** | `NO_AUTORIZADO` | Falta el token, no es un JWT, la firma no coincide, no tiene `exp` o venció. Trae `WWW-Authenticate: Bearer` | `"falta el token: Authorization: Bearer <token>"`, `"el token venció"` |
| **403** | `ROL_INSUFICIENTE` | Un `usuario` en `/stats` o en `/admin/*` | `"/stats es sólo para admin (tu rol: usuario)"` |
| 403 | `IMAGEN_NO_PERMITIDA` | Algún paso pide una imagen que no está en `SOBEL_IMAGENES_PERMITIDAS` | `"la imagen docker.io/alguien/minero:1 no está permitida"` |
| **404** | `JOB_INEXISTENTE` | El job no existe, venció (24 h) o es de otro usuario | `"no existe el job 3f6c2a9e-…"` |
| 404 | `RUTA_INEXISTENTE` | La ruta no existe, o el resultado `n` está fuera de rango | `"no existe la ruta /jobs/3f6c2a9e-…/resultado/7"` |
| 405 | `METODO_NO_PERMITIDO` | La ruta existe pero no acepta ese método. Trae `Allow` | `"DELETE no está permitido en /jobs/{id}"` |
| 409 | `JOB_NO_TERMINADO` | Se pidió un resultado de un job que no está `done` | `"el job está en processing (62.5 %)"` |
| **413** | `ARCHIVO_DEMASIADO_GRANDE` | Un archivo pasa de 100 MiB o de 100 megapíxeles, o el pedido pasa de 200 MiB | `"el archivo 2 (perro.jpg) supera los 104857600 bytes"` |
| 415 | `TIPO_DE_CONTENIDO` | `POST /jobs` sin `multipart/form-data` | `"el cuerpo tiene que ser multipart/form-data (llegó: application/json)"` |
| 422 | `PAYLOAD_INVALIDO` | Falta un campo, `operaciones` no es JSON válido, hay 0 o más de 3 pasos, o 0 o más de 50 archivos | `"el pedido no es válido"` + `detalles: [{"campo": "operaciones", "problema": "tiene entre 1 y 3 pasos, llegaron 4"}]` |
| 422 | `ARCHIVO_INVALIDO` | Un archivo no es PNG ni JPEG, o está corrupto | `"hay archivos que no son imágenes válidas"` + `detalles: [{"campo": "archivos[1]", "problema": "no es PNG ni JPEG"}]` |
| 422 | `OPERACION_NO_SOPORTADA` | La imagen de ese paso no ofrece esa operación | `"la imagen … no ofrece la operación 'rotar' (ofrece: sobel, grises, blur)"` |
| **429** | `DEMASIADOS_PEDIDOS` | Más de 120 pedidos por minuto del mismo usuario. Trae `Retry-After` | `"más de 120 pedidos por minuto: reintentá en 14 s"` |
| **500** | `ERROR_INTERNO` | Error inesperado. El detalle va al log, nunca al cliente | `"error interno del servidor"` |
| **503** | `SERVICIO_NO_DISPONIBLE` | Redis o RabbitMQ no responden (el backend sigue reintentando conectarse con backoff) | `"el servicio no puede aceptar jobs en este momento"` |
| 503 | `SIN_WORKERS` | No hay ningún worker activo de la imagen de algún paso. Trae `Retry-After` | `"no hay workers activos de la imagen …"` |

Una falla **durante** el procesamiento no es un código HTTP, porque el `POST` ya contestó `202`: queda en el estado
del job (`failed` o `dlq`, con su `error`).

Qué se puede reintentar: `429` y `503` (esperando `Retry-After`) y `500`. Los `4xx` no mejoran sin cambiar el pedido.

## 7. Lo que falta

Datos que **no** devolvemos y podríamos devolver:

| Dato | Por qué queda afuera |
|---|---|
| Costo estimado del job | Exige cruzar el tiempo de cada worker con el precio de su máquina y la parte del cluster que usó. El precio depende de la nube y de la región, y se conoce recién en la factura |
| Nodo, zona y región de cada worker | Devolvemos el nombre del worker. Agregarle la ubicación obliga a cada worker a consultar el metadata server de la nube, distinto en cada proveedor y vacío en k3s |
| Qué fragmentos se reasignaron desde la DLQ, y desde qué worker | Devolvemos la cantidad (`reintentos`). El detalle crece con fragmentos × intentos y está en los logs del reasignador |
| Tiempos de cada fragmento y de cada paso | Devolvemos promedios. Con 50 archivos × 3 pasos la respuesta tendría cientos de entradas; el detalle está en Prometheus |
| Historial de estados con la hora de cada cambio | Sólo guardamos `creado` y `terminado`. Las etapas intermedias se ven en `tiempos_s` |
| Posición en la cola y tiempo estimado de fin | Cambian todo el tiempo y dependen de cuántos workers haya en ese momento. Con `fragmentos.porcentaje` y `tiempos_s.cola` el cliente puede estimarlo |
| CPU y memoria que consumió el job | Los workers procesan fragmentos de varios jobs a la vez; repartir el consumo por job necesita medir cada fragmento. A nivel pod está en Grafana |
| Digest de la imagen que procesó cada fragmento | El pedido ya fija tag o digest. Si fija un tag y se republica con otro contenido, no se nota: por eso conviene pedir por digest |
| Progreso por paso | `completos` cuenta sólo los fragmentos que terminaron todos los pasos |
| Transparencia (canal alfa) de los PNG | El split pasa todo a RGB, que es lo que usan las tres operaciones: el resultado sale sin alfa |

## 8. SLO (para la matriz de R5)

Una celda de la matriz **cumple** si se cumplen las tres condiciones:

1. p95 de `POST /jobs` ≤ 1 s;
2. p95 de `tiempos_s.total` ≤ 30 s para pedidos de hasta 10 MB en total, y ≤ 120 s para los más grandes;
3. menos de 1 % de errores entre los pedidos válidos: `5xx`, más jobs que terminan `failed` o `dlq` sin que los provoquemos.

Los umbrales se revisan con la primera medición en la nube.

# Parte 2 — Interfaces internas

## 9. RabbitMQ

Las colas, los exchanges y sus argumentos se declaran **sólo** en `hit3/comun/colas.py`. Cada servicio lo llama al
conectarse: si dos servicios declaran la misma cola con argumentos distintos, RabbitMQ cierra el canal.

| Nombre | Tipo | Quién publica → quién consume |
|---|---|---|
| `sobel.split` | cola quorum | backend → split |
| `sobel.tareas.<imagen>` | cola quorum, una por imagen de worker, con DLX `sobel.dlx`, routing key `sobel.dlq` y `x-delivery-limit` | split, worker y reasignador → workers de esa imagen |
| `sobel.dlx` | exchange direct | lo usa RabbitMQ al descartar un mensaje |
| `sobel.dlq` | cola quorum, atada a `sobel.dlx` | RabbitMQ → reasignador |
| `sobel.dlq.agotados` | cola quorum | reasignador → backend (al reprocesar) |
| `sobel.resultados` | exchange fanout | worker (último paso) → joiner y frontend |
| `sobel.joiner` | cola quorum, atada a `sobel.resultados` | → joiner (compiten las réplicas) |
| (sin nombre) | cola exclusiva, atada a `sobel.resultados` | → frontend; una por réplica, se borra sola |

- `<imagen>` es la referencia completa de la imagen (`sobel.tareas.ghcr.io/chimera-sdypp/sobel-worker:1.0.0`).
- Todos declaran con `hit3/comun/colas.py` y el mismo `SOBEL_LIMITE_ENTREGAS`: si una cola ya existe con otros
  argumentos, RabbitMQ responde `PRECONDITION_FAILED`, y para cambiarlos hay que borrarla. RabbitMQ cuenta
  *reentregas*: con 3, un mensaje se entrega hasta 4 veces antes de ir a la DLQ.
- Quien publica a una `sobel.tareas.<imagen>` la declara antes. La declaración es idempotente y evita que un mensaje
  se pierda si todavía no arrancó ningún worker de esa imagen.
- Mensajes JSON (`content_type: application/json`) y persistentes (`delivery_mode: 2`). Los píxeles **no viajan
  por RabbitMQ**: van en Redis y el mensaje lleva la clave.
- **A la DLQ** llega un fragmento cuando el worker hace `nack` sin reencolar (error al procesarlo, timeout) o
  cuando el worker muere con el mensaje en la mano más veces que `x-delivery-limit` (crash, OOM).

**`sobel.split`**: `{"job_id": "3f6c2a9e-…"}`. El resto lo lee de `job:<id>`.

**`sobel.tareas.<imagen>`** (uno por fragmento y paso):

```json
{"job_id": "3f6c2a9e-…", "archivo": 0, "fragmento": 3, "paso": 1,
 "operaciones": [{"operacion": "grises", "imagen": "…"}, {"operacion": "sobel", "imagen": "…"}],
 "intento": 1, "encolado": 1761055391204}
```

| Campo | Qué es |
|---|---|
| `archivo`, `fragmento` | Índices desde 0 |
| `paso` | Índice en `operaciones` del paso a aplicar. La entrada está en `job:<id>:frag:<archivo>:<fragmento>:<paso>` |
| `operaciones` | La cadena completa, para que el worker sepa a qué cola mandar el paso siguiente |
| `intento` | Empieza en 1; el reasignador le suma 1 cada vez que lo vuelve a encolar |
| `encolado` | Hora de publicación en ms (epoch), para medir la espera en la cola |

**Qué hace el worker con cada tarea**, en este orden:

1. Lee la entrada, aplica `operaciones[paso].operacion` y escribe `job:<id>:frag:<a>:<f>:<paso+1>`.
2. Si quedan pasos, publica la misma tarea con `paso + 1` a la cola de la imagen del paso siguiente. Si era el último,
   publica el resultado en `sobel.resultados`.
3. Suma sus tiempos al job y se anota en `job:<id>:workers` (§10).
4. Hace `ack` y borra la entrada. Si se cae antes del `ack`, el paso se repite: como las claves se pisan con lo mismo,
   repetir no rompe nada; sólo los promedios de `tiempos_s` cuentan ese fragmento de más.

**`sobel.resultados`**:

```json
{"job_id": "3f6c2a9e-…", "archivo": 0, "fragmento": 3, "worker": "sobel-worker-7d9f-x2k4", "ms": 412, "intento": 1}
```

## 10. Redis

Todas las claves de un job empiezan con `job:<id>` y vencen a las 24 h (`SOBEL_TTL`).

| Clave | Tipo | Escribe → lee | Contenido |
|---|---|---|---|
| `job:<id>` | hash | ver abajo | `usuario`, `estado`, `creado` y `terminado` (ms), `operaciones` (JSON), `archivos`, `fragmentos_total`, `completos`, `en_dlq`, `reintentos`, `t_split_ms`, `t_join_ms`, `cola_ms` y `cola_n`, `proc_ms` y `proc_n` (suma y cantidad, para el promedio), `error` (JSON) |
| `job:<id>:workers` | set | worker → backend | Workers que procesaron algo |
| `job:<id>:entrada:<a>` | string | backend → split | El archivo original. El split lo borra |
| `job:<id>:geometria:<a>` | hash | split → joiner | `total` (fragmentos del archivo) y, por fragmento `<f>`, `"<filas_extra_arriba>,<filas>"` |
| `job:<id>:frag:<a>:<f>:<paso>` | string | split (paso 0), worker (paso k + 1) → worker, joiner | El fragmento en PNG. `paso` = cantidad de operaciones ya aplicadas |
| `job:<id>:listos:<a>` | set | joiner | Fragmentos del archivo que terminaron todos los pasos |
| `job:<id>:unidos` | set | joiner | Archivos ya unidos |
| `job:<id>:resultado:<a>` | string | joiner → backend | La imagen final en PNG |
| `jobs`, `jobs:<usuario>` | zset | backend | `job_id` con la hora de creación como puntaje |
| `terminados` | list | joiner, split, reasignador → backend | `"<estado> <ms totales>"` de los últimos 1000 jobs terminados (`LPUSH` + `LTRIM`), para `/stats` |
| `worker:<id>` | hash, vence a los 15 s | worker → backend | `<id>` es el hostname (pod o VM). `imagen`, `operaciones` (separadas por coma), `host`. El worker lo renueva cada 5 s |
| `limite:<usuario>:<minuto>` | string, vence a los 60 s | backend | Contador del límite de pedidos |

**Quién escribe qué en `job:<id>`:**

- **backend:** crea el hash (`usuario`, `estado=queued`, `creado`, `operaciones`, `archivos`) y las entradas. Al reprocesar
  la DLQ: `estado=processing`, borra `error` y `terminado`, y resta de `en_dlq`.
- **split:** `estado=processing`, `fragmentos_total`, `t_split_ms`, la geometría y los fragmentos del paso 0. Si un
  archivo no se puede decodificar: `estado=failed`, `error`, `terminado` y la entrada en `terminados`.
- **worker:** `cola_ms`, `cola_n`, `proc_ms`, `proc_n` (con `HINCRBY`) y `job:<id>:workers`.
- **joiner:** `completos`, los resultados, y al final `estado=done`, `terminado`, `t_join_ms` y la entrada en `terminados`.
  Borra los fragmentos del último paso después de unir.
- **reasignador:** `reintentos`. Cuando un fragmento agota los intentos: `en_dlq`, `estado=dlq`, `error`, `terminado` y la entrada en `terminados`.

**Bordes.** Sobel y blur necesitan los vecinos de cada píxel. El split le agrega a cada fragmento tantas filas extra
arriba y abajo como operaciones de vecindad tenga la cadena (`sobel` y `blur` cuentan; `grises` no; con un `blur` de más
de 3×3 harían falta más filas), y anota en
`geometria` cuántas puso arriba y cuántas filas le pertenecen. El worker procesa el fragmento entero, sin saber de
bordes, y el joiner recorta. Así el resultado es idéntico al de aplicar la cadena a la imagen entera, como en el Hit 1.

**Que el joiner una cada archivo una sola vez.** `SADD` del fragmento en `listos`; si devuelve 1 (es nuevo), suma 1 a
`completos`. Cuando el tamaño llega a `total`,
el que lo logra con `SADD` sobre `unidos` (devuelve 1) une el archivo. Si es el último archivo, pone `done`, `terminado`
y `t_join_ms`, y agrega la entrada a `terminados`.

## 11. Métricas

Cada servicio expone `GET /metrics` (formato Prometheus) en su puerto HTTP.

| Métrica | Tipo | Etiquetas | Quién |
|---|---|---|---|
| `sobel_tareas_procesadas_total` | counter | `servicio`, `operacion`, `resultado` (`ok` o `error`) | split (`operacion="split"`), worker (`sobel`, `grises`, `blur`), joiner (`join`), reasignador (`reasignar`, `agotar`) |
| `sobel_tiempo_tarea_segundos` | histograma | `servicio`, `operacion` | los mismos. De acá salen p50, p95 y p99 |
| `sobel_errores_total` | counter | `servicio`, `tipo` | todos |
| `sobel_tareas_en_cola` | gauge | `cola` | backend, cada 10 s |
| `sobel_tiempo_job_segundos` | histograma | — | joiner, al pasar un job a `done` |
| `sobel_pedidos_total` | counter | `ruta`, `codigo` | backend |

El plugin `rabbitmq_prometheus` (puerto 15692) da publicados, consumidos y en cola. Por defecto suma todas las colas;
para verlas una por una se usa `/metrics/per-object`.

## 12. Servicios y configuración

Un servicio por carpeta en `hit3/`. Todos escuchan HTTP en el **8080** (`/health` y `/metrics`; el backend y el frontend
además sus rutas). Imágenes privadas: `ghcr.io/chimera-sdypp/sobel-<servicio>:<tag>`.

El `/health` de cada servicio devuelve `{"<servicio>": "ok", "rabbitmq": "ok", "redis": "ok"}`: `200`, o `503` si alguna
conexión está caída. Las conexiones a RabbitMQ y Redis se reintentan con backoff de 1, 2, 4, 8… s, con tope de 30 s
(`hit3/comun/`).

| Variable | Quién | Default | Qué es |
|---|---|---|---|
| `RABBITMQ_URL` | todos | — | `amqp://usuario:clave@host:5672/` (Secret) |
| `REDIS_URL` | todos | — | `redis://:clave@host:6379/0` (Secret) |
| `SOBEL_PUERTO` | todos | `8080` | Puerto HTTP |
| `SOBEL_TTL` | todos | `86400` | Segundos que viven las claves de un job |
| `SOBEL_LIMITE_ENTREGAS` | todos | `3` | `x-delivery-limit` de las `sobel.tareas.<imagen>`. Igual en todos (§9) |
| `SOBEL_SECRETO` | backend | — | Firma de los JWT (Secret) |
| `SOBEL_IMAGENES_PERMITIDAS` | backend | — | Imágenes de worker permitidas, separadas por coma |
| `SOBEL_SERVICIOS` | backend | — | `nombre=url` separados por coma: la URL completa del `/health` de cada servicio que resume el público (`split=http://split:8080/health`). Sin `backend`, `redis`, `rabbitmq` ni `workers`, que los informa el backend |
| `SOBEL_PEDIDOS_POR_MINUTO` | backend | `120` | Límite por usuario |
| `SOBEL_PARTES` | split | `4` | Fragmentos por archivo (menos si la imagen tiene menos filas) |
| `SOBEL_IMAGEN` | worker | — | La referencia de su propia imagen; tiene que coincidir con la de `SOBEL_IMAGENES_PERMITIDAS` |
| `SOBEL_OPERACIONES` | worker | `sobel,grises,blur` | Las operaciones que ofrece |
| `SOBEL_TIMEOUT` | worker | `60` | Segundos máximos por fragmento; pasado ese tiempo, `nack` a la DLQ |
| `SOBEL_REINTENTOS_MAX` | reasignador | `3` | Intentos antes de mandar el fragmento a `sobel.dlq.agotados` |
