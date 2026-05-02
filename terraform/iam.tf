resource "google_service_account" "pipeline_sa" {
  account_id   = var.service_account_id
  display_name = "Urban Pipeline Service Account"
  description  = "Used by Dataproc, Airflow, and ingestion scripts"
  project      = var.project_id

  depends_on = [
    google_project_service.apis["iam.googleapis.com"]
  ]
}

locals {
  pipeline_sa_roles = [
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/dataproc.editor",
    "roles/dataproc.worker",
    "roles/pubsub.publisher",
    "roles/secretmanager.secretAccessor",
  ]
}

resource "google_project_iam_member" "pipeline_sa_bindings" {
  for_each = toset(local.pipeline_sa_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

output "service_account_email" {
  description = "Email of the pipeline service account"
  value       = google_service_account.pipeline_sa.email
}