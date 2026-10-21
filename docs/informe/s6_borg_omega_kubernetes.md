# Trabajo Práctico N° 3 — Sistemas Distribuidos y Paralelos
## Informe Individual: Salvador Baez (Rol: Infraestructura, Observabilidad y Carga)
### Análisis Teórico: *Borg, Omega y Kubernetes: Lecciones de una década de gestión de contenedores* (Burns et al., 2016)

---

## 1. Introducción y Contexto

A lo largo de más de quince años, Google desarrolló y evolucionó de manera interna sus plataformas de orquestación a gran escala para coordinar cargas de trabajo en centros de datos masivos sobre hardware genérico (*commodity hardware*). Dicha evolución se plasmó en tres generaciones sucesivas de orquestadores: **Borg**, **Omega** y **Kubernetes**.

El artículo fundacional *"Borg, Omega, and Kubernetes: Lessons learned from three container-management systems over a decade"* (Burns, Grant, Oppenheimer, Brewer & Wilkes, 2016) sintetiza los aciertos de diseño, los callejones sin salida y las transformaciones conceptuales que culminaron en el desarrollo y liberación de Kubernetes como estándar *open-source*. 

El presente informe analiza la trayectoria evolutiva entre estos tres sistemas, profundiza en las diferencias arquitectónicas fundamentales (particularmente la transición desde el estado compartido con concurrencia optimista hacia el control declarativo orientado a nivel), y vincula directamente estos conceptos con las decisiones de infraestructura y orquestación implementadas en el **Hit #3 (Arquitectura Distribuida)** y **Hit #4 (Observabilidad)** de nuestro sistema distribuido de procesamiento de imágenes.

---

## 2. De Borg a Omega: El paso del Monolito al Estado Compartido

### 2.1 Borg: La orquestación monolítica centralizada
Diseñado a principios de la década de 2000, **Borg** fue el orquestador pionero de Google concebido para unificar bajo una sola infraestructura tanto los servicios de producción de baja latencia y alta prioridad (ej. motor de búsqueda web, Gmail) como los trabajos por lotes intensivos en cómputo (*batch processing*, ej. MapReduce).

* **Arquitectura:** El núcleo de control de un clúster de Borg (*Borg cell*) era el **Borgmaster**, un proceso monolítico centralizado. La persistencia y el consenso se garantizaban mediante un grupo de réplicas que utilizaban el algoritmo **Paxos**, donde una réplica líder coordinaba las escrituras y atendía las consultas. En cada nodo físico residía un agente local denominado **Borglet**, responsable de iniciar, supervisar y aislar los procesos.
* **Limitaciones fundamentales:**
  1. *Acoplamiento monolítico:* La lógica de almacenamiento de estado, la API de usuario y el planificador (*scheduler*) estaban empaquetados dentro del mismo proceso ejecutable. A medida que surgieron nuevas políticas de asignación y requerimientos de scheduling específicos para diferentes tipos de carga, el monolito del Borgmaster se convirtió en un severo cuello de botella de escalabilidad y mantenimiento.
  2. *Monopolio del planificador:* Al existir una única cola y un único motor de planificación global, agregar heurísticas complejas degradaba la tasa de asignación de tareas por segundo (*throughput* de scheduling).

```
[Borg: Monolito Centralizado]
 Usuarios / Clientes
        │
        ▼
┌──────────────────────────────────────────────┐
│                  Borgmaster                  │
│  ┌──────────────┐  ┌──────────────────────┐  │
│  │ API & Estado │◄─┼─► Planificador Único │  │
│  └──────┬───────┘  └──────────────────────┘  │
│         │ (Paxos)                            │
└─────────┼────────────────────────────────────┘
          ▼
    Borglets (Nodos)
```

### 2.2 Omega: Descentralización y Concurrencia Optimista (OCC)
Para resolver la rigidez del Borgmaster, a principios de la década de 2010 se concibió **Omega**, adoptando una arquitectura diametralmente opuesta basada en el principio de **estado compartido** (*shared-state architecture*).

* **Almacenamiento transaccional centralizado:** Omega eliminó por completo el planificador centralizado. En su lugar, el corazón del sistema era una base de datos de estado global replicada mediante Paxos.
* **Múltiples planificadores paralelos:** Múltiples instancias de planificadores autónomos, cada uno optimizado para una clase particular de trabajo (por ejemplo, batch vs. streaming vs. servicios web interactivos), operaban de manera concurrente.
* **Control de Concurrencia Optimista (OCC):** Cada planificador trabajaba sobre una instantánea local y asíncrona del estado del clúster. Al decidir la asignación de un contenedor a un nodo, el planificador intentaba confirmar la decisión mediante una transacción atómica contra el almacén central de Paxos:
  * Si los recursos del nodo continuaban libres, la transacción se confirmaba (*commit*).
  * Si otro planificador había asignado concurrentemente esos mismos recursos, la transacción fallaba por conflicto (*abort*); el planificador debía invalidar su vista local, sincronizar el estado fresco y recalcular la planificación.
* **Trade-offs y complejidades:** Aunque Omega demostró un escalamiento masivo para cargas heterogéneas, la arquitectura de estado compartido directo presentaba desventajas significativas:
  1. La ausencia de una capa de mediación o API unificada obligaba a que todos los componentes internos conocieran el esquema interno de la base de datos y respetaran las complejas invariantes de consistencia.
  2. En escenarios de saturación o alta contención de recursos, la tasa de abortos por OCC crecía exponencialmente, generando trabajo computacional redundante (*livelock* transaccional).

```
[Omega: Estado Compartido con Concurrencia Optimista]
  Planificador Batch     Planificador Web      Planificador ML
          │                      │                    │
          └──────────────┬───────┴────────────────────┘
                         │ (Transacciones OCC)
                         ▼
             ┌───────────────────────┐
             │ Almacén de Estado     │
             │ Central (Paxos Store) │
             └───────────────────────┘
```

---

## 3. Kubernetes: Síntesis y Nuevo Paradigma

Cuando los ingenieros de Google comenzaron a diseñar **Kubernetes** en 2014, capitalizaron una década completa de lecciones aprendidas en producción con Borg y Omega. Kubernetes no fue una reimplementación directa de ninguno de los dos, sino una síntesis que rescató las mejores virtudes de cada uno descartando sus deficiencias estructurales:

| Dimensión | Borg | Omega | Kubernetes |
|---|---|---|---|
| **Punto focal de arquitectura** | Monolito centralizado (*Borgmaster*) | Almacén de estado compartido con múltiples schedulers directos | Servidor de API centralizado + bucles de reconciliación desacoplados |
| **Manejo de concurrencia** | Bloqueo pesimista centralizado | Concurrencia optimista libre a nivel almacén (OCC) | Concurrencia optimista regulada mediante versiones de recursos en la API (`resourceVersion`) |
| **Almacén de estado** | Paxos interno embebido | Base de datos Paxos central | Base de datos clave-valor distribuida independiente (`etcd`, con consenso Raft) |
| **Modelo de control** | Imperativo (órdenes dirigidas al nodo) | Transaccional (commits sobre estado) | Declarativo (convergencia orientada a nivel) |
| **Unidad de ejecución** | Tarea / Alloc | Tarea | **Pod** (grupo atómico de contenedores) |
| **Descubrimiento y red** | Puertos dinámicos por nodo + nombres en Chubby | Puertos dinámicos | Red plana (IP única por Pod) + **Services** e Ingress |

```
[Kubernetes: Control Declarativo Reconciliado]
        kubectl / Clientes / CI-CD
                     │
                     ▼
         ┌───────────────────────┐
         │     kube-apiserver    │◄──── etcd (Consenso Raft)
         └───▲───────────────▲───┘
             │               │
    (Observa / Reconcilia)   │ (Asigna Pods pendientes)
             │               │
  ┌──────────┴────────┐  ┌───┴─────────────┐
  │  kube-controller- │  │  kube-scheduler │
  │      manager      │  └─────────────────┘
  └───────────────────┘
             │
             ▼
       Kubelet (Nodos) ──► Pods (Contenedores)
```

---

## 4. Lecciones Arquitectónicas Clave (Burns et al.)

### 4.1 Bucle de Reconciliación: *Level-Triggered* vs. *Edge-Triggered*
Uno de los aportes teóricos más profundos del paper es la justificación de por qué Kubernetes se estructuró como un sistema **orientado a nivel** (*level-triggered*) en lugar de **orientado a eventos o flancos** (*edge-triggered*):

* **Sistemas orientados a eventos (*edge-triggered*):** Reaccionan puntualmente a transiciones de estado mediante el envío de mensajes o eventos discretos (ej. *"se destruyó el contenedor A"*, *"llegó una orden de encendido"*). En sistemas distribuidos a gran escala sujetos a particiones de red, caídas de nodos y reinicios de procesos, los eventos pueden perderse, duplicarse o llegar desordenados. Si un componente pierde un evento de terminación, el sistema puede quedar permanentemente desincronizado respecto a la realidad física.
* **Sistemas orientados a nivel (*level-triggered*):** En Kubernetes, los componentes operan bajo el **patrón del controlador** (*Controller Pattern*), estructurado sobre un bucle infinito de reconciliación idempotente:
  $$\text{Acción} = f(\text{Estado Deseado } [\text{spec}], \text{Estado Real Actual } [\text{status}])$$
  El controlador no necesita saber qué eventos ocurrieron en el pasado; periódicamente (y ante notificaciones de cambio) compara la especificación deseada con la realidad observada en el clúster y calcula la acción mínima necesaria para reducir la divergencia a cero. Si un mensaje de red se pierde, la siguiente iteración del bucle detecta la discrepancia y aplica la corrección. Esta propiedad confiere a Kubernetes su naturaleza **autosanadora** (*self-healing*).

### 4.2 De *Allocs* a la abstracción de *Pods*
En Borg, la unidad básica de ejecución era una tarea individual (un único binario corriendo en un contenedor chroot/cgroups). Con el tiempo, se evidenció que muchas aplicaciones complejas requerían procesos auxiliares colocalizados: agentes de registro de logs (*log rotators*), recolectores de métricas, proxies de red o descargadores de certificados. Borg introdujo tardíamente la noción de *alloc* para reservar recursos compartidos entre varias tareas en una misma máquina.

Kubernetes elevó este concepto a ciudadana de primer orden mediante la abstracción del **Pod**:
1. **Espacios de nombres compartidos:** Todos los contenedores dentro de un Pod comparten el mismo *network namespace* (comparten la dirección IP del Pod, el espacio de puertos y pueden comunicarse entre sí mediante `localhost`) y los mismos volúmenes de almacenamiento declarados.
2. **Ciclo de vida atómico:** El Pod se programa, arranca y termina como una unidad indivisible en un mismo nodo físico. Esto habilita patrones de diseño modulares fundamentales en microservicios:
   * *Sidecar:* Procesos de soporte que complementan al contenedor principal (ej. exportador de métricas o recolector de logs).
   * *Init Containers:* Contenedores transitorios que preparan el entorno (migraciones de esquemas, descarga de secretos) antes de iniciar la lógica central.

### 4.3 De Puertos Dinámicos a Direccionamiento Plano y *Services*
En Borg y en los sistemas basados en contenedores de primera generación (como Apache Mesos o las configuraciones tradicionales de Docker Compose con port-binding), las tareas compartían la IP del host y debían solicitar al planificador la asignación dinámica de puertos libres (ej. el servicio A recibía el puerto 31452 en el nodo 4, y el puerto 28901 en el nodo 8). Esto introducía una severa fragilidad:
* Los clientes debían resolver la ubicación de cada instancia a través de un servicio de nombres (*service discovery*) dinámico y complejo.
* Las aplicaciones no podían vincularse a puertos estándar de protocolo (como el puerto 80 o 443).

Kubernetes impuso el modelo de **IP por Pod** (*flat networking*), donde cada Pod recibe una dirección IP enrutable única dentro de la red del clúster. Sobre esta base, introdujo la abstracción de **Service**:
* Una dirección IP virtual estable (*ClusterIP*) y un nombre DNS canónico dentro del clúster que no cambian a lo largo del tiempo.
* Desacoplamiento total entre el consumidor y los productores mediante **Labels y Label Selectors**: el Service balancea el tráfico automáticamente entre todos los Pods activos cuyas etiquetas coincidan con su selector, independientemente de cuántas veces se destruyan, escalen o reubiquen.

---

## 5. Aplicación Práctica en Nuestro Proyecto (Grupo Cerberus)

Los principios teóricos expuestos por Burns et al. guiaron de forma directa las decisiones de ingeniería implementadas en la infraestructura del **Hit #3** (21 recursos de Kubernetes en `hit3/k8s/`) y en la observabilidad del **Hit #4** (`hit4/`):

### 5.1 Estado vs. Efimeridad: StatefulSets frente a Deployments
Siguiendo la premisa de que los sistemas de control deben modelar explícitamente los requerimientos de identidad y estado de las cargas de trabajo:
* **Servicios de datos con estado (RabbitMQ y Redis):** Se orquestaron como `StatefulSet`. A diferencia de un `Deployment`, el StatefulSet garantiza identidades de red ordinales y determinísticas (`rabbitmq-0`, `redis-0`), persistencia desacoplada del ciclo de vida del contenedor a través de `PersistentVolumeClaims` (PVCs), y secuencia estricta en el encendido y apagado de réplicas.
* **Componentes computacionales sin estado (Backend, Split, Joiner, Reasignador, Workers):** Se orquestaron como `Deployment` desacoplados. La lógica del backend implementa una arquitectura enteramente *stateless* (requisito R4), replicada en 3 instancias idénticas detrás de un `ClusterIP` y expuesta por el Ingress de Traefik. Si una réplica del backend o un worker colapsa, el ReplicaSet controller detecta la discrepancia a nivel de estado deseado y levanta un reemplazo de inmediato.

### 5.2 Topología y Aislamiento de Recursos: *Node Selectors* y *Pools*
En alineación con la segregación de cargas de trabajo que realizaba Borg entre servicios de infraestructura y aplicaciones de usuario, estructuramos el clúster en dos grupos lógicos de nodos:
* **Infraestructura (`sobel/infra=true`):** Reservado para los componentes críticos y de soporte que requieren estabilidad y persistencia (RabbitMQ, Redis, Prometheus y Grafana).
* **Aplicaciones (`sobel/apps=true`):** Asignado a los procesos de cómputo y transformación (Backend API, Splitter, Joiner, Reasignador y Workers de Sobel y Grises).

Para evitar que el clúster k3s local mononodo colapsara por incompatibilidad de etiquetas (donde la asignación secuencial de una etiqueta común sobreescribe el valor anterior), implementamos una estrategia de **etiquetas booleanas independientes** (`sobel/infra: "true"` y `sobel/apps: "true"`), permitiendo que en entornos de desarrollo ambos roles convivan en un único nodo sin fricción, pero garantizando que en un despliegue multinodo o en Google Kubernetes Engine (GKE) los pods se aíslen físicamente en *node pools* especializados.

### 5.3 Resiliencia en Sondas de Salud: Desacople de *Liveness* y *Readiness*
Uno de los problemas más críticos observados en sistemas distribuidos es el fenómeno de **tormenta de reinicios en cascada**: si la sonda de vida (*liveness probe*) de un componente depende del estado de salud de sus servicios downstream, una degradación transitoria de la red o el arranque lento de una base de datos provoca que Kubernetes mate y reinicie cíclicamente todos los pods dependientes, impidiendo la recuperación del sistema.

En base a este aprendizaje:
* **Liveness Probe:** Configurada como `tcpSocket` al puerto de escucha local (`8080`). Verifica únicamente que el proceso del servicio esté vivo y atendiendo a nivel de socket de red del contenedor.
* **Readiness Probe:** Configurada mediante sondeo HTTP al endpoint `/health`. Evalúa la disponibilidad funcional del servicio (incluyendo la conectividad activa con RabbitMQ y Redis). Si RabbitMQ se encuentra transitoriamente ocupado o reiniciándose, el Pod simplemente se desvincula del balanceador del Service para no recibir tráfico de clientes, pero **no es reiniciado**, permitiendo que el bucle de reconexión con backoff exponencial restablezca el enlace sin penalización.

### 5.4 Observabilidad Declarativa: ServiceMonitors y Prometheus Operator
En el **Hit #4**, la recolección de métricas adopta la filosofía declarativa de Kubernetes:
* En lugar de mantener archivos de configuración estáticos de Prometheus con direcciones IP o nombres de host fijos, utilizamos el recurso personalizado (*CRD*) `ServiceMonitor` del `kube-prometheus-stack`.
* El `ServiceMonitor` utiliza selectores de etiquetas (`matchLabels: { app: ... }`) para descubrir dinámicamente todos los endpoints que exponen métricas en `/metrics`. Cuando un deployment escala de 1 a 10 réplicas, el operador de Prometheus incorpora automáticamente las nuevas instancias al ciclo de raspado (*scrape loop*) sin intervención manual.
* Para evitar la distorsión métrica en el dashboard de Grafana —donde 3 réplicas del backend reportan concurrentemente la profundidad de la cola de RabbitMQ—, diseñamos consultas PromQL con agregación por máximo (`max(sobel_tareas_en_cola)` en lugar de `sum()`), garantizando coherencia en el monitoreo de capacidad.

---

## 6. Síntesis de Entrega (Sección S6 para Consolidación)

Esta sección provee los elementos que se integran en el informe consolidado final a cargo de Tomás (rol T7):

### 6.1 Resumen del Stack de Observabilidad (Hit #4)
* **Infraestructura:** Helm release de `kube-prometheus-stack` desplegado en el espacio de nombres `infra` con afinidad hacia `sobel/infra=true`.
* **Dashboards:** Tablero de Grafana exportado con 19 paneles organizados en 4 filas temáticas:
  1. *Infraestructura:* Consumo de CPU y Memoria por Pod y por Nodo.
  2. *Pipeline de RabbitMQ:* Mensajes publicados vs. consumidos por segundo, y profundidad en cola (sobel.split, sobel.tareas.sobel, sobel.tareas.grises, sobel.joiner, sobel.dlq).
  3. *Métricas de Negocio:* Tareas procesadas exitosamente por operación y tasa de errores.
  4. *Rendimiento y SLOs:* Latencia de respuesta de la API Gateway (p50, p95, p99) y conteo dinámico de workers activos.
* **Reglas de Alerta Activas:**
  * `SobelColaSaturada`: Dispara una alerta de severidad `warning` si `sobel_tareas_en_cola > 50` durante más de 1 minuto.
  * `SobelWorkerInactivo`: Dispara una alerta de severidad `critical` si la cantidad de workers activos cae a 0 mientras existen tareas pendientes encoladas.

### 6.2 Resumen de Pruebas de Carga con k6 (Hit #3 / R5)
* **Suite de pruebas:** Script modular `hit3/carga/carga_matriz.js` con soporte para matrices multidimensionales:
  * $V_A$ Archivos: 1, 10, 50.
  * $V_B$ Operaciones: 1 (sobel), 2 (sobel + grises).
  * $V_C$ Usuarios Concurrentes: 1, 10, 50 VUs con rampa de aceleración controlada.
  * Token de autenticación Bearer dinámico para el tráfico legítimo y simulación de bot no autorizado sin token.
  * Inyección controlada de casos anómalos: operaciones inválidas y archivos binarios corruptos.
* **Runner automatizado:** `correr_matriz.sh` para ejecución reproducible con k6 binario o contenedor `grafana/k6`, volcando resultados en JSON estructurado y resumen Markdown.

---

## 7. Registro de Uso de Herramientas de Inteligencia Artificial

En cumplimiento con las normas de integridad académica y transparencia de autoría establecidas por la cátedra (Prof. Petrocelli):

1. **Herramientas utilizadas:**
   * **Gemini (Google DeepMind):** Empleado como asistente de programación en pares para la conceptualización de manifiestos declarativos de Kubernetes, resolución de conflictos de etiquetado de nodos en k3s, refinamiento de expresiones regulares y agregaciones en PromQL para Grafana, y estructuración de la suite de pruebas en JavaScript para k6.
2. **Alcance y supervisión:**
   * Todo el código generado, configuraciones de infraestructura YAML y análisis teóricos fueron revisados, validados contra la especificación oficial de la cátedra y ejecutados en entornos de prueba locales antes de su incorporación al repositorio.
   * La concepción arquitectónica, la asignación de responsabilidades de equipo y la interpretación crítica de los textos académicos corresponden a la autoría de los integrantes del Grupo Cerberus.

---
*Salvador Baez — Grupo Cerberus (SDyPP 2026)*
