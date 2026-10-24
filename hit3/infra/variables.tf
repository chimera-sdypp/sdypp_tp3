variable "proyecto" {
  description = "ID del proyecto de GCP"
  type        = string
}

variable "region" {
  type    = string
  default = "southamerica-east1"
}

variable "zona" {
  description = "Cluster zonal: GCP no cobra el plano de control de un cluster zonal por cuenta de facturación"
  type        = string
  default     = "southamerica-east1-a"
}

variable "nodos_infra" {
  description = "Nodos del pool de infraestructura (RabbitMQ, Redis, Prometheus y Grafana)"
  type        = number
  default     = 1
}

variable "nodos_apps" {
  description = "Nodos del pool de aplicaciones (backend, frontend, split, joiner, reasignador)"
  type        = number
  default     = 2
}

variable "tipo_nodo" {
  type    = string
  default = "e2-standard-2"
}

variable "workers" {
  description = "VMs de workers fuera del cluster. El pipeline de workers la ajusta según la cola pendiente"
  type        = number
  default     = 0
}

variable "tipo_worker" {
  type    = string
  default = "e2-standard-2"
}

variable "imagen_worker" {
  description = "Imagen del worker en Artifact Registry (salida `registry` de infra/base)"
  type        = string
}
