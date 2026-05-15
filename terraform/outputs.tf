output "project_id" {
  description = "GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "Default region"
  value       = var.region
}

output "data_buckets" {
  description = "Map of data bucket names by purpose"
  value = {
    for k, v in google_storage_bucket.data : k => v.name
  }
}

output "bq_datasets" {
  description = "Map of BigQuery dataset IDs by purpose"
  value = {
    for k, v in google_bigquery_dataset.datasets : k => v.dataset_id
  }
}
