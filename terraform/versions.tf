terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  backend "gcs" {
    bucket = "urban-pipeline-kd-2026-tf-state"
    prefix = "terraform/state"
  }
}