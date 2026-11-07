# Lo que se crea una sola vez y a mano (con gcloud auth application-default login), porque incluye la
# identidad con la que después el CI aplica el resto: no puede crearse a sí misma.

locals {
  cuenta_master = "system:serviceaccount:sobel-hit1:sobel-master" # la ServiceAccount del pod del master
  apis = [
    "artifactregistry.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "secretmanager.googleapis.com",
    "sts.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

# Registry privado de la nube: las VMs y los nodos bajan las imágenes con su cuenta de servicio, sin claves (R1).
resource "google_artifact_registry_repository" "imagenes" {
  repository_id = "sobel"
  format        = "DOCKER"
  location      = var.region
  depends_on    = [google_project_service.apis]
}

# GitHub Actions se autentica por OIDC: cada job recibe un token que vence en una hora.
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github"
  display_name              = "GitHub Actions"
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
    "attribute.ref"        = "assertion.ref"
  }
  attribute_condition = "assertion.repository == \"${var.repositorio_github}\""
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "ci" {
  account_id   = "sobel-ci"
  display_name = "CI de sdypp_tp3 (Terraform e imágenes)"
}

resource "google_project_iam_member" "ci" {
  for_each = toset(var.roles_ci)
  project  = var.proyecto
  role     = each.value
  member   = "serviceAccount:${google_service_account.ci.email}"
}

resource "google_service_account_iam_member" "ci_desde_github" {
  service_account_id = google_service_account.ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.repositorio_github}"
}

# El master del Hit #2 corre en k3s y crea las VMs con Terraform. GCP confía en los tokens de service account que
# firma ese cluster (se le sube su clave pública, porque el emisor no es accesible desde Internet) y el pod los cambia
# por credenciales temporales de sobel-ci, la misma cuenta que aplica hit2/infra desde el CI. Sin claves guardadas.
resource "google_iam_workload_identity_pool" "k3s" {
  count                     = var.k3s_jwks == null ? 0 : 1
  workload_identity_pool_id = "k3s-local"
  display_name              = "k3s local (Hit 2)"
  depends_on                = [google_project_service.apis]
}

resource "google_iam_workload_identity_pool_provider" "k3s" {
  count                              = var.k3s_jwks == null ? 0 : 1
  workload_identity_pool_id          = google_iam_workload_identity_pool.k3s[0].workload_identity_pool_id
  workload_identity_pool_provider_id = "k3s-local"
  attribute_mapping                  = { "google.subject" = "assertion.sub" }
  attribute_condition                = "assertion.sub == \"${local.cuenta_master}\""
  oidc {
    issuer_uri = "https://kubernetes.default.svc.cluster.local"
    jwks_json  = var.k3s_jwks
  }
}

resource "google_service_account_iam_member" "ci_desde_k3s" {
  count              = var.k3s_jwks == null ? 0 : 1
  service_account_id = google_service_account.ci.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principal://iam.googleapis.com/${google_iam_workload_identity_pool.k3s[0].name}/subject/${local.cuenta_master}"
}
