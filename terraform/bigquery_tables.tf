# -----------------------------------------------------------------------------
# Raw layer tables — partitioned and clustered for incremental ingestion.
#
# Partition key is the date the data is *about*, not the date it was ingested.
# This lets DELETE-then-APPEND backfills (in load_to_bigquery.py) prune
# precisely to a single day's partition without scanning the full table.
# -----------------------------------------------------------------------------

resource "google_bigquery_table" "taxi_trips" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw"].dataset_id
  table_id   = "taxi_trips"

  description = "Raw NYC yellow-taxi trips, partitioned by pickup_date."

  schema = file("${path.module}/schemas/taxi_trips.json")

  time_partitioning {
    type  = "DAY"
    field = "pickup_date"
  }

  require_partition_filter = false


  clustering = ["pickup_hour", "pickup_location_id"]

  # Allow Terraform to delete and recreate during the Day 8 migration;
  # this can be set to false later for prod-like protection.
  deletion_protection = false

  depends_on = [
    google_bigquery_dataset.datasets
  ]
}

resource "google_bigquery_table" "weather_hourly" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw"].dataset_id
  table_id   = "weather_hourly"

  description = "Hourly NYC weather from Open-Meteo, partitioned by weather_date."

  schema = file("${path.module}/schemas/weather_hourly.json")

  time_partitioning {
    type  = "DAY"
    field = "weather_date"
  }

  require_partition_filter = false


  deletion_protection = false

  depends_on = [
    google_bigquery_dataset.datasets
  ]
}

resource "google_bigquery_table" "taxi_trips_enriched" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["staging"].dataset_id
  table_id   = "taxi_trips_enriched"

  description = "Enriched taxi trips joined with hourly weather. Partitioned by pickup_date, clustered by pickup_hour and pickup_location_id."

  schema = file("${path.module}/schemas/taxi_trips_enriched.json")

  time_partitioning {
    type  = "DAY"
    field = "pickup_date"
  }

  require_partition_filter = false

  clustering = ["pickup_hour", "pickup_location_id"]

  deletion_protection = false

  depends_on = [
    google_bigquery_dataset.datasets
  ]
}
