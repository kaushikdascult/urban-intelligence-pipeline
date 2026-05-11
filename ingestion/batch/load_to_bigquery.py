"""
Load raw taxi (parquet) and weather (JSON) data from GCS into BigQuery.

- Taxi: native parquet load, schema auto-detected.
- Weather: column-major JSON re-shaped into row-major NDJSON, then loaded.
Both go into the `raw` dataset, one table per source.
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery, storage

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "urban-pipeline-kd-2026")
RAW_BUCKET = os.getenv("RAW_BUCKET", "urban-pipeline-kd-2026-raw")
RAW_DATASET = os.getenv("RAW_DATASET", "raw")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Source GCS objects (created by taxi_ingestion.py and weather_ingestion.py)
TAXI_GCS_URI = (
    f"gs://{RAW_BUCKET}/taxi/ingestion_date={INGESTION_DATE}/taxi_trips.parquet"
)
WEATHER_GCS_URI = (
    f"gs://{RAW_BUCKET}/weather/ingestion_date={INGESTION_DATE}/weather_nyc.json"
)

# Target tables
TAXI_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.taxi_trips"
WEATHER_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.weather_hourly"

# Path inside the staging bucket for the reshaped weather NDJSON
RESHAPED_WEATHER_OBJECT = (
    f"weather/_reshaped/ingestion_date={INGESTION_DATE}/weather_hourly.ndjson"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Taxi loader (parquet → BigQuery)
# -----------------------------------------------------------------------------
def load_taxi_to_bigquery(client: bigquery.Client) -> None:
    """Load parquet from GCS into raw.taxi_trips. Replaces table if it exists."""
    log.info(f"Loading taxi parquet from {TAXI_GCS_URI}")
    log.info(f"Target: {TAXI_TABLE}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_uri(TAXI_GCS_URI, TAXI_TABLE, job_config=job_config)
    log.info(f"BQ job started: {job.job_id}")
    job.result()  # blocking wait

    table = client.get_table(TAXI_TABLE)
    log.info(f"Loaded {table.num_rows:,} rows into {TAXI_TABLE}")
    log.info(f"Table size: {table.num_bytes / 1024 / 1024:.2f} MB")


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


def load_weather_to_bigquery(client: bigquery.Client) -> None:
    """Pull raw JSON from GCS, reshape, write NDJSON back, load into BQ."""
    log.info(f"Reading raw weather JSON from {WEATHER_GCS_URI}")
    storage_client = storage.Client(project=PROJECT_ID)

    bucket_name = WEATHER_GCS_URI.split("/")[2]
    object_name = "/".join(WEATHER_GCS_URI.split("/")[3:])
    raw_bytes = storage_client.bucket(bucket_name).blob(object_name).download_as_bytes()
    raw_json = json.loads(raw_bytes.decode("utf-8"))

    rows = reshape_weather_to_ndjson(raw_json)

    ndjson_uri = upload_ndjson_to_gcs(rows, RAW_BUCKET, RESHAPED_WEATHER_OBJECT)
    log.info(f"Reshaped NDJSON at {ndjson_uri}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    log.info(f"Loading NDJSON into {WEATHER_TABLE}")
    job = client.load_table_from_uri(ndjson_uri, WEATHER_TABLE, job_config=job_config)
    log.info(f"BQ job started: {job.job_id}")
    job.result()

    table = client.get_table(WEATHER_TABLE)
    log.info(f"Loaded {table.num_rows:,} rows into {WEATHER_TABLE}")
    log.info(f"Table size: {table.num_bytes / 1024:.2f} KB")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("Loading raw data into BigQuery")
    log.info(f"Project: {PROJECT_ID}")
    log.info(f"Dataset: {RAW_DATASET}")
    log.info(f"Ingestion date: {INGESTION_DATE}")
    log.info("=" * 60)

    bq_client = bigquery.Client(project=PROJECT_ID)

    log.info("\n--- Taxi ---")
    load_taxi_to_bigquery(bq_client)

    log.info("\n--- Weather ---")
    load_weather_to_bigquery(bq_client)

    log.info("\n" + "=" * 60)
    log.info("Done.")
    log.info("Tables created:")
    log.info(f"  {TAXI_TABLE}")
    log.info(f"  {WEATHER_TABLE}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
