# Van como variables del repositorio en GitHub (no son secretos): GCP_PROVEEDOR_WIF y GCP_CUENTA_CI.
output "proveedor_wif" {
  value = google_iam_workload_identity_pool_provider.github.name
}

output "cuenta_ci" {
  value = google_service_account.ci.email
}

output "registry" {
  value = "${var.region}-docker.pkg.dev/${var.proyecto}/${google_artifact_registry_repository.imagenes.repository_id}"
}

# Para la configuración de credenciales del master del Hit #2 en k3s (hit2/k8s/).
output "proveedor_k3s" {
  value = var.k3s_jwks == null ? null : google_iam_workload_identity_pool_provider.k3s[0].name
}
