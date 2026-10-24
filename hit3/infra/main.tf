# Hit #3: GKE con un pool de infraestructura y otro de aplicaciones, y los workers en VMs aparte.

locals {
  registro = split("/", var.imagen_worker)[0]
}

# Los nodos bajan imágenes de Artifact Registry con esta cuenta: no hace falta imagePullSecret (R1).
resource "google_service_account" "nodos" {
  account_id   = "sobel-nodos"
  display_name = "Nodos de GKE"
}

resource "google_project_iam_member" "nodos" {
  for_each = toset(["roles/artifactregistry.reader", "roles/logging.logWriter", "roles/monitoring.metricWriter"])
  project  = var.proyecto
  role     = each.value
  member   = "serviceAccount:${google_service_account.nodos.email}"
}

resource "google_container_cluster" "sobel" {
  name                     = "sobel"
  location                 = var.zona
  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false
  networking_mode          = "VPC_NATIVE"
  ip_allocation_policy {}

  # Workload Identity: un pod puede actuar como una cuenta de servicio de GCP sin claves.
  workload_identity_config {
    workload_pool = "${var.proyecto}.svc.id.goog"
  }

  release_channel {
    channel = "REGULAR"
  }
}

resource "google_container_node_pool" "infra" {
  name       = "infra"
  cluster    = google_container_cluster.sobel.id
  node_count = var.nodos_infra

  node_config {
    machine_type    = var.tipo_nodo
    service_account = google_service_account.nodos.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = { "sobel/infra" = "true" }
    # Sólo entra lo que tolera el taint (los manifiestos de k8s/ y de hit4/), así las apps no le sacan memoria.
    taint {
      key    = "sobel/infra"
      value  = "true"
      effect = "NO_SCHEDULE"
    }
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

# Sin taint: además de las apps, acá corren los pods del sistema (kube-dns, metrics-server).
resource "google_container_node_pool" "apps" {
  name       = "apps"
  cluster    = google_container_cluster.sobel.id
  node_count = var.nodos_apps

  node_config {
    machine_type    = var.tipo_nodo
    service_account = google_service_account.nodos.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    labels          = { "sobel/apps" = "true" }
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

# ---------------------------------------------------------------- workers fuera del cluster

resource "google_service_account" "worker" {
  account_id   = "sobel-worker-hit3"
  display_name = "Workers del Hit #3"
}

# Las URLs de RabbitMQ y Redis (con su clave) las carga crear_secretos.sh; la VM las lee al arrancar.
resource "google_secret_manager_secret" "conexion" {
  for_each  = toset(["sobel-rabbitmq-url", "sobel-redis-url"])
  secret_id = each.value
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "worker_lee" {
  for_each  = google_secret_manager_secret.conexion
  secret_id = each.value.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_project_iam_member" "worker_lee_imagenes" {
  project = var.proyecto
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.worker.email}"
}

resource "google_compute_instance_template" "worker" {
  name_prefix  = "sobel-worker-"
  machine_type = var.tipo_worker
  tags         = ["sobel-worker"]

  disk {
    source_image = "debian-cloud/debian-12"
    disk_size_gb = 10
  }

  # Sin IP pública: llegan a RabbitMQ y Redis por la VPC (Services internos) y salen por Cloud NAT.
  network_interface {
    network = "default"
  }

  service_account {
    email  = google_service_account.worker.email
    scopes = ["cloud-platform"]
  }

  shielded_instance_config {
    enable_secure_boot = true
  }

  metadata = {
    enable-oslogin = "TRUE"
    startup-script = templatefile("${path.module}/arranque.sh.tftpl", {
      registro = local.registro
      imagen   = var.imagen_worker
    })
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_compute_instance_group_manager" "workers" {
  name               = "sobel-workers"
  zone               = var.zona
  base_instance_name = "sobel-worker"
  target_size        = var.workers
  version {
    instance_template = google_compute_instance_template.worker.id
  }
}

# Las VMs sin IP pública necesitan NAT para bajar paquetes e imágenes.
resource "google_compute_router" "nat" {
  name    = "sobel-nat"
  region  = var.region
  network = "default"
}

resource "google_compute_router_nat" "nat" {
  name                               = "sobel-nat"
  router                             = google_compute_router.nat.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}
