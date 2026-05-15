locals {
  bq_datasets = {
    raw     = "Raw ingested data from GCS (taxi_raw, weather_raw)"
    staging = "PySpark transformation output joined and enriched"
    marts   = "Final dbt models — fact and dimension tables"
  }
}

resource "google_bigquery_dataset" "datasets" {
  for_each = local.bq_datasets

  dataset_id    = each.key
  friendly_name = "Urban Pipeline ${each.key}"
  description   = each.value
  location      = var.bq_location
  project       = var.project_id

  default_table_expiration_ms = 7776000000

  delete_contents_on_destroy = true

  depends_on = [
    google_project_service.apis["bigquery.googleapis.com"]
  ]
}
