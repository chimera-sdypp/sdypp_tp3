# Manifiestos de Kubernetes — Hit #3 (Sobel Asincrónico)

Este directorio contiene los manifiestos de Kubernetes para desplegar la arquitectura completa del **Hit #3**, tanto en clústeres locales (**k3s**) como en la nube.

---

## 1. Arquitectura y Grupos de Nodos

La arquitectura divide los pods en dos grupos de nodos mediante `nodeSelector` y `tolerations`:

1. **`sobel/infra=true` (Infraestructura persistente y observabilidad):**
   - **RabbitMQ StatefulSet:** broker de mensajería con volumen persistente (`5Gi`) montado en `/var/lib/rabbitmq` e imagen `rabbitmq:4-management`. Expone AMQP (`5672`), Management (`15672`) y el plugin `rabbitmq_prometheus` (`15692`).
   - **Redis StatefulSet:** almacén en memoria con persistencia en disco (`appendonly yes`, PVC `10Gi`), contraseña obligatoria (`requirepass`) y límites aumentados (`2Gi` RAM). Expone el puerto `6379`.
   - **Observabilidad (Hit #4):** stack de Prometheus, Alertmanager y Grafana (desplegado vía Helm en este mismo pool).

2. **`sobel/apps=true` (Microservicios y procesamiento):**
   - **Backend Deployment:** 3 réplicas del API Gateway HTTP (`2Gi` RAM, puerto `8080`), validación previa de pedidos y métricas.
   - **Split Deployment:** divide imágenes entrantes en fragmentos con filas de borde (`2Gi` RAM para imágenes de hasta 100 megapíxeles).
   - **Worker Deployment:** `sobel-worker`, que ofrece las tres operaciones (`sobel`, `grises`, `blur`), con `1Gi` RAM y un Service headless para scraping de Prometheus.
   - **Joiner Deployment:** 2 réplicas concurrentes (`2Gi` RAM) que consumen de `sobel.joiner`, coordinan la unión atómica en Redis y publican resultados.
   - **Reasignador Deployment:** consume la cola de fallos `sobel.dlq`, evalúa reintentos y deriva a `sobel.dlq.agotados` tras superar el umbral (`SOBEL_REINTENTOS_MAX: 3`).
   - **Frontend Deployment:** interfaz web y dashboard de progreso en vivo (puerto `8080`).

> **Diseño de Probes:**
> * **`readinessProbe`:** `httpGet: /health` en el puerto `8080`. Indica si el servicio está listo para recibir tráfico (depende de que RabbitMQ y Redis estén conectados).
> * **`livenessProbe`:** `tcpSocket: 8080`. Comprueba únicamente si el proceso del contenedor está vivo. Esto evita cascadas de reinicios destructivos si RabbitMQ o Redis demoran en arrancar o están reconectando con backoff.

---

## 2. Instrucciones de Despliegue en k3s

### Paso 0: Etiquetar los Nodos

En un clúster k3s local de nodo único (o máquina de desarrollo), etiquetar el nodo con ambas etiquetas simultáneamente:

```bash
# Obtener el nombre del nodo
NODE_NAME=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')

# Aplicar las etiquetas de los pools (dos claves independientes)
kubectl label nodes "$NODE_NAME" sobel/infra=true sobel/apps=true --overwrite
```

> **En clústeres multi-nodo o nube (GKE / EKS):** Cada node pool se etiqueta de forma independiente (`sobel/infra=true` para el pool de infraestructura y `sobel/apps=true` para el pool de cómputo).

---

### Paso 1: Generar los Secretos Locales

Los secretos **nunca se versionan en Git** (cumpliendo R1 y auditoría de gitleaks). Ejecute el script provisto para crearlos en el clúster con credenciales dinámicas de alta entropía:

```bash
cd hit3/k8s
./crear_secretos.sh
```

El script genera automáticamente el Secret `sobel-secretos` en el namespace `sobel` con:
- `RABBITMQ_USER` y `RABBITMQ_PASS` generados aleatoriamente (evitando defaults inseguros).
- `RABBITMQ_URL` (`amqp://<user>:<pass>@rabbitmq.sobel.svc.cluster.local:5672/`).
- `REDIS_PASS` generado aleatoriamente y `REDIS_URL` (`redis://:<pass>@redis.sobel.svc.cluster.local:6379/0`).
- `SOBEL_SECRETO` (clave criptográfica de 32 caracteres para tokens JWT HS256).
- Copia local de respaldo en `.env.secrets` (con permisos `600`, ignorado por `.gitignore`).
- Si `sobel-secretos` ya existe, reusa sus valores: volver a correrlo no cambia las claves (RabbitMQ toma su usuario sólo al crear el volumen).

*(Opcional)* Si desea descargar imágenes privadas de GitHub Container Registry (`ghcr.io`), defina las variables antes de correr el script:
```bash
GHCR_USER="tu-usuario" GHCR_PAT="tu-token-ghcr" ./crear_secretos.sh
```

---

### Paso 2: Despliegue Atómico con Kustomize

El archivo `kustomization.yaml` permite aplicar todo el catálogo en orden correcto y de manera atómica:

```bash
kubectl apply -k hit3/k8s/
```

O verificar previamente el YAML renderizado:
```bash
kubectl kustomize hit3/k8s/
```

---

### Paso 3: Verificar el Estado de los Componentes

1. **Verificar que los pods estén corriendo:**
   ```bash
   kubectl get pods -n sobel -o wide
   ```

2. **Verificar los StatefulSets y sus PVCs persistentes:**
   ```bash
   kubectl get statefulset,pvc -n sobel
   ```

3. **Verificar los Services e Ingress:**
   ```bash
   kubectl get svc,ingress -n sobel
   ```

---

## 3. Catálogo de Manifiestos

| Archivo | Tipo de Recurso | Descripción |
|---|---|---|
| `00-namespace.yaml` | `Namespace` | Namespace aislado `sobel` |
| `01-rabbitmq.yaml` | `ConfigMap`, `Service`, `StatefulSet` | RabbitMQ 4 con plugin Prometheus y PVC de 5Gi en `sobel/infra=true` |
| `02-redis.yaml` | `Service`, `StatefulSet` | Redis 7 con requirepass, persistencia AOF y PVC de 10Gi en `sobel/infra=true` |
| `03-backend.yaml` | `Service`, `Deployment` | 3 réplicas del backend API Gateway (`2Gi` RAM) en `sobel/apps=true` |
| `04-split.yaml` | `Service`, `Deployment` | Particionado de imágenes (`2Gi` RAM) en `sobel/apps=true` |
| `05-workers.yaml` | `Service`, `Deployment` | Workers (`sobel`, `grises`, `blur`) con Services headless en `sobel/apps=true` |
| `06-joiner.yaml` | `Service`, `Deployment` | 2 réplicas concurrentes del ensamblador (`2Gi` RAM) en `sobel/apps=true` |
| `07-reasignador.yaml` | `Service`, `Deployment` | Manejador de fallos y reenvíos desde DLQ en `sobel/apps=true` |
| `08-frontend.yaml` | `Service`, `Deployment` | Interfaz web de usuario y monitoreo en `sobel/apps=true` |
| `09-ingress.yaml` | `Ingress` | Enrutamiento HTTP hacia backend (`/jobs`, `/admin`, `/stats`, `/health`) y frontend (`/`) |
| `crear_secretos.sh` | Bash Script | Generación segura de secrets con credenciales dinámicas sin defaults fijos |
| `kustomization.yaml` | Kustomize | Índice declarativo para despliegue atómico |
