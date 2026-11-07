# sdypp_tp3

Trabajo Práctico 3 — Cloud Computing · Grupo Cerberus (Salvador Baez, Mateo Nomico, Tomás Resnik).
Informe: [docs/informe/informe-tp3-cerberus.pdf](docs/informe/informe-tp3-cerberus.pdf) · versión entregada: [v1.1.0](https://github.com/chimera-sdypp/sdypp_tp3/releases/tag/v1.1.0).

| Hit | Qué hay | Evidencia | Video |
|---|---|---|---|
| Parte 0 | Cluster k3s local | [Prerrequisitos](hit1/README.md#prerrequisitos-cumplidos-parte-0) | — |
| [Hit #0 — Patrones de RabbitMQ](TP3/queue/ex5/README.md) | [Ex1](TP3/queue/ex1/README.md) (P2P), [Ex2](TP3/queue/ex2/README.md) (Pub/Sub), [Ex3](TP3/queue/ex3/README.md) (DLQ), [Ex4](TP3/queue/ex4/README.md) (Retry Backoff), [Ex5](TP3/queue/ex5/README.md) (Síntesis) | En cada README | [Ex1](https://github.com/chimera-sdypp/sdypp_tp3/releases/download/v1.1.0/hit0-ex1-demo.mp4) · [Ex2](https://github.com/chimera-sdypp/sdypp_tp3/releases/download/v1.1.0/hit0-ex2-demo.mp4) · [Ex3](https://github.com/chimera-sdypp/sdypp_tp3/releases/download/v1.1.0/hit0-ex3-demo.mp4) · [Ex4](https://github.com/chimera-sdypp/sdypp_tp3/releases/download/v1.1.0/hit0-ex4-demo.mp4) |
| [Hit #1 — Operador de Sobel](hit1/README.md) | Etapas 1, 2 y 3 | [Tolerancia a fallos](hit1/README.md#etapa-3--tolerancia-a-fallos) | [Video](https://github.com/chimera-sdypp/sdypp_tp3/releases/download/v1.1.0/tp3-hit1-demo.mp4) |
| [Hit #2 — Sobel con offloading a la nube](hit2/README.md) | Master en k3s que crea VMs en GCP según la cola | [Terraform y corridas en GCP](hit2/README.md#evidencia-de-terraform) | [Video](https://github.com/chimera-sdypp/sdypp_tp3/releases/download/v1.1.0/tp3-hit2-demo.mp4) |
| [Hit #3 — Sobel asincrónico en Kubernetes](hit3/README.md) | API, colas, DLQ y despliegue · en la nube: <http://8.232.111.42> (GKE) | [R1, R4, caso de falla](hit3/evidencia/) · [carga con k6](hit3/carga/resultados/) | — |
| [Hit #4 — Observabilidad](hit4/README.md) | Prometheus, Grafana, alertas y Loki | [Disparar una alerta](hit4/README.md#8-procedimiento-para-disparar-una-alerta-a-propósito-evidencia) | — |

Infraestructura en GCP (Terraform y CI): [infra/](infra/README.md).
