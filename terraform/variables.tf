variable "project_id" {
  description = "GCP project ID where all resources will be created"
  type        = string
}

variable "region" {
  description = "Default GCP region for regional resources"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location (US is multi-region)"
  type        = string
  default     = "US"
}

variable "bucket_prefix" {
  description = "Prefix for GCS bucket names. Must be globally unique."
  type        = string
}

variable "service_account_id" {
  description = "Service account ID (without domain suffix)"
  type        = string
  default     = "urban-pipeline-sa"
}

variable "labels" {
  description = "Labels applied to all resources for billing tracking"
  type        = map(string)
  default = {
    project     = "urban-pipeline"
    environment = "dev"
    managed_by  = "terraform"
  }
}
