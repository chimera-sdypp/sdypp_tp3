terraform {
  required_version = ">= 1.9"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
  # terraform init -backend-config="bucket=<proyecto>-tfstate"
  backend "gcs" {
    prefix = "hit2"
  }
}

provider "google" {
  project = var.proyecto
  region  = var.region
  zone    = var.zona
}
