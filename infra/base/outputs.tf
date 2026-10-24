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
