"""
Load raw taxi (parquet) and weather (JSON) data from GCS into BigQuery.

- Taxi: native parquet load, schema is Terraform-managed.
- Weather: column-major JSON re-shaped into row-major NDJSON, then loaded.

Idempotent: each day's load runs DELETE-then-APPEND scoped to the ingestion
date, so re-running the same date replaces only that partition. New dates
accumulate. Designed for the Day 8 backfill: 31 daily runs produce a
complete monthly table.

Accepts --ingestion-date for incremental runs. Defaults to today (UTC).

Usage:
    python load_to_bigquery.py                          # today (UTC)
    python load_to_bigquery.py --ingestion-date 2022-01-15
"""

import argparse
import json
import logging
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

from google.cloud import bigquery, storage

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "urban-pipeline-kd-2026")
RAW_BUCKET = os.getenv("RAW_BUCKET", "urban-pipeline-kd-2026-raw")
RAW_DATASET = os.getenv("RAW_DATASET", "raw")

# Target tables (date-independent; Terraform-managed schemas)
TAXI_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.taxi_trips"
WEATHER_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.weather_hourly"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI args. --ingestion-date defaults to today (UTC)."""
    parser = argparse.ArgumentParser(
        description="Load one day of raw taxi+weather data from GCS into BigQuery.",
    )
    parser.add_argument(
        "--ingestion-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=datetime.now(timezone.utc).date(),
        help="ISO date (YYYY-MM-DD) to load. Defaults to today (UTC).",
    )
    return parser.parse_args()


def delete_partition(
    client: bigquery.Client, table: str, partition_column: str, ingestion_date: date
) -> int:
    """DELETE rows for the given date from a partitioned table.

    Returns the number of rows deleted. Cheap because BQ prunes to the
    single partition. No-op for empty partitions; required to make
    same-day re-runs idempotent.
    """
    sql = f"DELETE FROM `{table}` WHERE {partition_column} = @ingestion_date"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ingestion_date", "DATE", ingestion_date),
        ]
    )
    log.info(f"DELETE FROM {table} WHERE {partition_column} = '{ingestion_date}'")
    job = client.query(sql, job_config=job_config)
    job.result()
    deleted = job.num_dml_affected_rows or 0
    log.info(f"Deleted {deleted:,} pre-existing rows for {ingestion_date}")
    return deleted


# -----------------------------------------------------------------------------
# Taxi loader (parquet → BigQuery)
# -----------------------------------------------------------------------------
def load_taxi_to_bigquery(
    client: bigquery.Client, taxi_gcs_uri: str, ingestion_date: date
) -> None:
    """DELETE the date's existing partition, then APPEND the parquet from GCS.

    Schema is Terraform-managed (terraform/schemas/taxi_trips.json), so we
    don't pass autodetect or an explicit schema — BQ validates the parquet
    against the existing table schema and rejects on mismatch.
    """
    delete_partition(client, TAXI_TABLE, "pickup_date", ingestion_date)

    log.info(f"Loading taxi parquet from {taxi_gcs_uri}")
    log.info(f"Target: {TAXI_TABLE}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_uri(taxi_gcs_uri, TAXI_TABLE, job_config=job_config)
    log.info(f"BQ load job: {job.job_id}")
    job.result()

    log.info(f"Loaded {job.output_rows:,} rows for partition {ingestion_date}")


# -----------------------------------------------------------------------------
# Weather loader (columnar JSON → NDJSON → BigQuery)
# -----------------------------------------------------------------------------
def reshape_weather_to_ndjson(raw_json: dict) -> list[dict]:
    """Convert Open-Meteo's column-major hourly arrays into row-major records."""
    hourly = raw_json["data"]["hourly"]
    metadata = raw_json["ingestion_metadata"]

    columns = list(hourly.keys())  # ['time', 'temperature_2m', ...]
    n_rows = len(hourly["time"])

    rows = []
    for i in range(n_rows):
        row = {col: hourly[col][i] for col in columns}
        # Add metadata columns to every row for lineage
        row["city"] = metadata["city"]
        row["latitude"] = metadata["latitude"]
        row["longitude"] = metadata["longitude"]
        row["ingested_at_utc"] = metadata["ingested_at_utc"]
        # Derive partition key from the hourly timestamp.
        # Open-Meteo returns ISO 8601 strings like "2022-01-15T00:00".
        row["weather_date"] = row["time"][:10]
        rows.append(row)

    log.info(f"Reshaped {n_rows} hourly records (columnar -> row-wise)")
    return rows


def upload_ndjson_to_gcs(records: list[dict], bucket: str, object_name: str) -> str:
    """Write records as NDJSON to a temp file, upload to GCS, return URI."""
    storage_client = storage.Client(project=PROJECT_ID)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".ndjson", delete=False, encoding="utf-8"
    ) as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
        local_path = Path(f.name)

    log.info(f"Uploading reshaped NDJSON to gs://{bucket}/{object_name}")
    bkt = storage_client.bucket(bucket)
    blob = bkt.blob(object_name)
    blob.upload_from_filename(str(local_path), content_type="application/x-ndjson")

    local_path.unlink()
    return f"gs://{bucket}/{object_name}"


def load_weather_to_bigquery(
    client: bigquery.Client,
    weather_gcs_uri: str,
    reshaped_weather_object: str,
    ingestion_date: date,
) -> None:
    """Pull raw JSON from GCS, reshape, write NDJSON back, DELETE+APPEND into BQ."""
    log.info(f"Reading raw weather JSON from {weather_gcs_uri}")
    storage_client = storage.Client(project=PROJECT_ID)

    bucket_name = weather_gcs_uri.split("/")[2]
    object_name = "/".join(weather_gcs_uri.split("/")[3:])
    raw_bytes = storage_client.bucket(bucket_name).blob(object_name).download_as_bytes()
    raw_json = json.loads(raw_bytes.decode("utf-8"))

    rows = reshape_weather_to_ndjson(raw_json)
    ndjson_uri = upload_ndjson_to_gcs(rows, RAW_BUCKET, reshaped_weather_object)
    log.info(f"Reshaped NDJSON at {ndjson_uri}")

    delete_partition(client, WEATHER_TABLE, "weather_date", ingestion_date)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    log.info(f"Loading NDJSON into {WEATHER_TABLE}")
    job = client.load_table_from_uri(ndjson_uri, WEATHER_TABLE, job_config=job_config)
    log.info(f"BQ load job: {job.job_id}")
    job.result()

    log.info(f"Loaded {job.output_rows} rows for partition {ingestion_date}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main(ingestion_date: date | None = None):
    """Load taxi + weather raw data for the given date into BigQuery.

    When called from CLI, ingestion_date is None and we parse argv.
    When called from Airflow, the caller passes ingestion_date directly.
    """
    if ingestion_date is None:
        args = parse_args()
        ingestion_date = args.ingestion_date

    iso = ingestion_date.isoformat()
    taxi_gcs_uri = f"gs://{RAW_BUCKET}/taxi/ingestion_date={iso}/taxi_trips.parquet"
    weather_gcs_uri = f"gs://{RAW_BUCKET}/weather/ingestion_date={iso}/weather_nyc.json"
    reshaped_weather_object = (
        f"weather/_reshaped/ingestion_date={iso}/weather_hourly.ndjson"
    )

    log.info("=" * 60)
    log.info("Loading raw data into BigQuery")
    log.info(f"Project: {PROJECT_ID}")
    log.info(f"Dataset: {RAW_DATASET}")
    log.info(f"Ingestion date: {iso}")
    log.info("=" * 60)

    bq_client = bigquery.Client(project=PROJECT_ID)

    log.info("\n--- Taxi ---")
    load_taxi_to_bigquery(bq_client, taxi_gcs_uri, ingestion_date)

    log.info("\n--- Weather ---")
    load_weather_to_bigquery(
        bq_client, weather_gcs_uri, reshaped_weather_object, ingestion_date
    )

    log.info("\n" + "=" * 60)
    log.info("Done.")
    log.info(f"Tables updated for partition {iso}:")
    log.info(f"  {TAXI_TABLE}")
    log.info(f"  {WEATHER_TABLE}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
