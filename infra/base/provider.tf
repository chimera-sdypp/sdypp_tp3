terraform {
  required_version = ">= 1.9"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
  # El bucket se pasa al iniciar: terraform init -backend-config="bucket=<proyecto>-tfstate".
  # GCS bloquea el estado mientras dura un plan o un apply.
  backend "gcs" {
    prefix = "base"
  }
}

provider "google" {
  project = var.proyecto
  region  = var.region
}
