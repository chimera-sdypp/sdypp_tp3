output "ips" {
  value = google_compute_instance.worker[*].network_interface[0].access_config[0].nat_ip
}

# Para el master: python hit1/master.py --workers "localhost:8080,<esto>" entrada salida
output "workers" {
  value = join(",", [for ip in google_compute_instance.worker[*].network_interface[0].access_config[0].nat_ip : "${ip}:8080"])
}
