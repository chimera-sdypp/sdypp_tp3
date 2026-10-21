output "cluster" {
  value = google_container_cluster.sobel.name
}

# gcloud container clusters get-credentials <cluster> --zone <zona>
output "zona" {
  value = var.zona
}

output "grupo_workers" {
  description = "Para escalar los workers: gcloud compute instance-groups managed resize <grupo> --size N"
  value       = google_compute_instance_group_manager.workers.name
}
