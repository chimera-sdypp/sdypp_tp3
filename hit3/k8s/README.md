# Manifiestos de Kubernetes — Hit #3 (Sobel Asincrónico)

Este directorio contiene los manifiestos de Kubernetes para desplegar la arquitectura completa del **Hit #3**, tanto en clústeres locales (**k3s**) como en la nube.

---

## 1. Arquitectura y Grupos de Nodos

La arquitectura divide los pods en dos grupos de nodos mediante `nodeSelector` y `tolerations`:

1. **`pool=infra` (Infraestructura persistente y observabilidad):**
   - **RabbitMQ StatefulSet:** broker de mensajería con volumen persistente (`2Gi`) montado en `/var/lib/rabbitmq` e imagen `rabbitmq:4-management`. Expone AMQP (`5672`), Management (`15672`) y el plugin `rabbitmq_prometheus` (`15692`).
   - **Redis StatefulSet:** almacén en memoria con persistencia en disco (`appendonly yes`, PVC `2Gi`) montado en `/data` e imagen `redis:7-alpine`. Expone el puerto `6379`.
   - **Observabilidad (Hit #4):** stack de Prometheus y Grafana (desplegado vía Helm en este mismo pool).

2. **`pool=apps` (Microservicios sin estado y procesamiento):**
   - **Backend Deployment:** 3 réplicas sin estado del API Gateway HTTP (puerto `8080`).
   - **Split Deployment:** divide imágenes entrantes en fragmentos con filas de borde (puerto `8080`).
   - **Workers Deployments:** pods por operación (`sobel-worker`, `grises-worker`) que consumen fragmentos y computan transformaciones (puerto `8080`).
   - **Joiner Deployment:** 2 réplicas que compiten en `sobel.joiner`, coordinan la unión atómica en Redis y publican resultados (puerto `8080`).
   - **Reasignador Deployment:** consume la cola de fallos `sobel.dlq`, evalúa reintentos y deriva a `sobel.dlq.agotados` tras superar el umbral (puerto `8080`).
   - **Frontend Deployment:** interfaz web y dashboard de progreso en vivo (puerto `8080`).

---

## 2. Instrucciones de Despliegue en k3s

### Paso 0: Etiquetar los Nodos

En un clúster k3s local de nodo único (o máquina de pruebas), etiquetar el nodo para que acepte tanto pods de infraestructura como de aplicaciones:

```bash
# Obtener el nombre del nodo
NODE_NAME=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')

# Aplicar las etiquetas de los pools
kubectl label nodes "$NODE_NAME" pool=infra pool=apps --overwrite
```

> **En clústeres multi-nodo o nube:** Cada nodo se etiqueta según su propósito (ej. nodos de cómputo con `pool=apps` y nodos de soporte con `pool=infra`).

---

### Paso 1: Generar los Secretos Locales

Los secretos **nunca se versionan en Git** (cumpliendo R1 y auditoría de gitleaks). Ejecute el script provisto para crearlos en el clúster:

```bash
cd hit3/k8s
./crear_secretos.sh
```

El script genera automáticamente el Secret `sobel-secretos` en el namespace `sobel` con:
- `RABBITMQ_URL` (`amqp://guest:guest@rabbitmq.sobel.svc.cluster.local:5672/`)
- `REDIS_URL` (`redis://redis.sobel.svc.cluster.local:6379/0`)
- `SOBEL_SECRETO` (clave para tokens JWT HS256)
- Credenciales de acceso por defecto.

*(Opcional)* Si desea descargar imágenes privadas de GitHub Container Registry (`ghcr.io`), defina las variables antes de correr el script:
```bash
GHCR_USER="mi-usuario" GHCR_PAT="ghp_xxx" ./crear_secretos.sh
```

---

### Paso 2: Desplegar los Manifiestos

Aplique la infraestructura y todos los microservicios con Kustomize o kubectl:

```bash
# Opción A: con Kustomize (recomendada)
kubectl apply -k hit3/k8s/

# Opción B: aplicando todos los manifiestos
kubectl apply -f hit3/k8s/
```

---

### Paso 3: Verificación del Despliegue

1. **Verificar el estado de los Pods y StatefulSets:**
   ```bash
   kubectl get pods,statefulsets,services -n sobel -o wide
   ```

2. **Verificar los Volúmenes Persistentes (PVCs):**
   ```bash
   kubectl get pvc -n sobel
   ```
   Debe mostrar `rabbitmq-data-rabbitmq-0` y `redis-data-redis-0` en estado `Bound`.

3. **Verificar el Health Check Global:**
   ```bash
   kubectl port-forward svc/backend 8080:8080 -n sobel
   curl http://localhost:8080/health
   ```
   Respuesta esperada:
   ```json
   {"codigo": 200, "contenido": {"backend": "ok", "redis": "ok", "rabbitmq": "ok", ...}}
   ```

4. **Acceso a la interfaz de RabbitMQ Management:**
   ```bash
   kubectl port-forward svc/rabbitmq 15672:15672 -n sobel
   ```
   Abrir en el navegador: [http://localhost:15672](http://localhost:15672) (credenciales `guest`/`guest`).

5. **Acceso a Métricas de RabbitMQ (Prometheus):**
   ```bash
   kubectl port-forward svc/rabbitmq 15692:15692 -n sobel
   curl http://localhost:15692/metrics
   ```

---

## 3. Catálogo de Manifiestos

| Archivo | Tipo de Recurso | Descripción |
|---|---|---|
| `00-namespace.yaml` | `Namespace` | Namespace aislado `sobel`. |
| `crear_secretos.sh` | Bash Script | Generador seguro de `sobel-secretos` y `ghcr-secret`. |
| `01-rabbitmq.yaml` | `ConfigMap`, `Service`, `StatefulSet` | RabbitMQ 4 con plugin Prometheus y PVC de 2Gi en `pool=infra`. |
| `02-redis.yaml` | `Service`, `StatefulSet` | Redis 7 con persistencia AOF y PVC de 2Gi en `pool=infra`. |
| `03-backend.yaml` | `Service`, `Deployment` | 3 réplicas del API Gateway HTTP en `pool=apps`. |
| `04-split.yaml` | `Service`, `Deployment` | Servicio de división de imágenes en fragmentos. |
| `05-workers.yaml` | `Deployment` (x2) | Workers especializados por operación (`sobel`, `grises`). |
| `06-joiner.yaml` | `Service`, `Deployment` | 2 réplicas para el reensamblado concurrente de imágenes. |
| `07-reasignador.yaml` | `Service`, `Deployment` | Consumidor de `sobel.dlq` y reintento resiliente. |
| `08-frontend.yaml` | `Service`, `Deployment` | Web UI y dashboard de progreso. |
| `09-ingress.yaml` | `Ingress` | Enrutamiento perimetral para API (`/jobs`, `/admin`, `/health`) y Web (`/`). |
| `kustomization.yaml` | `Kustomization` | Agrupador para despliegues atómicos. |
