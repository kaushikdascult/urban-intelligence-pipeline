locals {
  data_buckets = {
    raw     = "Raw data landing zone (taxi parquet, weather JSON)"
    staging = "PySpark transformation output before BigQuery load"
    scripts = "PySpark scripts and JARs for Dataproc submission"
  }
}

resource "google_storage_bucket" "data" {
  for_each = local.data_buckets

  name     = "${var.bucket_prefix}-${each.key}"
  location = var.region
  project  = var.project_id

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = true

  depends_on = [
    google_project_service.apis["storage.googleapis.com"]
  ]
}