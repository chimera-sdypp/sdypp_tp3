variable "proyecto" {
  description = "ID del proyecto de GCP"
  type        = string
}

variable "region" {
  description = "Región de todos los recursos"
  type        = string
  default     = "southamerica-east1"
}

variable "repositorio_github" {
  description = "Único repositorio que puede pedir credenciales por OIDC"
  type        = string
  default     = "chimera-sdypp/sdypp_tp3"
}

variable "roles_ci" {
  description = "Roles de la cuenta con la que el CI aplica hit2/infra y hit3/infra"
  type        = list(string)
  default = [
    "roles/compute.admin",
    "roles/container.admin",
    "roles/iam.serviceAccountAdmin",
    "roles/iam.serviceAccountUser",
    "roles/resourcemanager.projectIamAdmin",
    "roles/secretmanager.admin",
    "roles/storage.objectAdmin",
    "roles/artifactregistry.writer",
  ]
}

variable "k3s_jwks" {
  description = "Clave pública del k3s donde corre el master del Hit #2 (kubectl get --raw /openid/v1/jwks); null = sin Workload Identity para k3s"
  type        = string
  default     = null
}
