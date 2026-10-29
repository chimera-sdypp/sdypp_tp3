# Workers de desborde del Hit #2 (cloud bursting): VMs que existen sólo mientras hay una imagen que procesar.
# Ciclo: provisioning (apply) → bootstrap (arranque.sh) → deploy (pull de la imagen) → join (el master usa
# la salida `workers`) → teardown (apply con cantidad = 0).

locals {
  registro = split("/", var.imagen)[0]
}

resource "google_service_account" "worker" {
  account_id   = "sobel-worker-hit2"
  display_name = "Workers del Hit #2"
}

# Sólo puede leer imágenes: no hay claves en la VM, baja la imagen con esta identidad.
resource "google_project_iam_member" "worker_lee_imagenes" {
  project = var.proyecto
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.worker.email}"
}

# Los workers del Hit #1 no tienen autenticación: sólo los alcanza la red local.
resource "google_compute_firewall" "worker" {
  name          = "sobel-hit2-worker"
  network       = "default"
  source_ranges = [var.ip_permitida]
  target_tags   = ["sobel-hit2-worker"]
  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }
}

resource "google_compute_instance" "worker" {
  count        = var.cantidad
  name         = "sobel-hit2-worker-${count.index}"
  machine_type = var.tipo_maquina
  tags         = ["sobel-hit2-worker"]

  boot_disk {
    initialize_params {
      image = "cos-cloud/cos-stable" # Container-Optimized OS: Docker ya instalado
      size  = 10
    }
  }

  network_interface {
    network = "default"
    access_config {} # IP pública efímera, para que el master local llegue
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
      imagen   = var.imagen
    })
  }
}
