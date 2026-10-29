variable "proyecto" {
  description = "ID del proyecto de GCP"
  type        = string
}

variable "region" {
  type    = string
  default = "southamerica-east1"
}

variable "zona" {
  type    = string
  default = "southamerica-east1-a"
}

variable "cantidad" {
  description = "Workers en la nube. En 0 no hay ninguna VM: el escalador del master la sube según la cola y la vuelve a 0"
  type        = number
  default     = 0
}

variable "tipo_maquina" {
  type    = string
  default = "e2-standard-2"
}

variable "imagen" {
  description = "Imagen del worker del Hit #1 en Artifact Registry (salida `registry` de infra/base)"
  type        = string
}

variable "ip_permitida" {
  description = "IP pública de la red local, en CIDR (x.x.x.x/32): el master es el único que llega al 8080"
  type        = string
}
