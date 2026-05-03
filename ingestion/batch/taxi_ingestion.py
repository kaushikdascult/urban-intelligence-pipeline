"""
Taxi data ingestion: BigQuery public dataset → GCS as Parquet.

Pulls NYC Yellow Taxi trips from January 2022, applies basic cleaning,
and lands them in the raw GCS bucket partitioned by ingestion date.
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.cloud import bigquery, storage

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "urban-pipeline-kd-2026")
RAW_BUCKET = os.getenv("RAW_BUCKET", "urban-pipeline-kd-2026-raw")
ROW_LIMIT = int(os.getenv("TAXI_ROW_LIMIT", "500000"))

PUBLIC_DATASET = "bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022"

INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
GCS_OBJECT_NAME = f"taxi/ingestion_date={INGESTION_DATE}/taxi_trips.parquet"

LOCAL_TEMP = Path("temp_taxi_trips.parquet")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def query_taxi_data(client: bigquery.Client, limit: int) -> pd.DataFrame:
    """Query BigQuery public taxi dataset and return as pandas DataFrame."""
    log.info(f"Querying {PUBLIC_DATASET} for {limit:,} rows...")

    sql = f"""
    SELECT
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code,
        store_and_fwd_flag,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        imp_surcharge,
        airport_fee,
        total_amount,
        pickup_location_id,
        dropoff_location_id
    FROM `{PUBLIC_DATASET}`
    WHERE pickup_datetime >= '2022-01-01'
      AND pickup_datetime <  '2022-02-01'
    LIMIT {limit}
    """

    df = client.query(sql).to_dataframe()
    log.info(f"Pulled {len(df):,} rows from BigQuery")
    return df


def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning: drop nulls, derive helper columns, filter anomalies."""
    log.info("Cleaning data...")

    initial = len(df)
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime", "fare_amount"])

    df["trip_duration_minutes"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
    ).round(2)
    df["pickup_date"] = df["pickup_datetime"].dt.date
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df["day_of_week"] = df["pickup_datetime"].dt.day_name()

    df = df[(df["trip_duration_minutes"] > 0) & (df["trip_duration_minutes"] < 24 * 60)]
    df = df[df["fare_amount"] >= 0]

    final = len(df)
    log.info(f"Cleaned: {initial:,} -> {final:,} rows ({initial - final:,} dropped)")
    return df


def write_parquet(df: pd.DataFrame, local_path: Path) -> None:
    """Write the dataframe as Parquet locally."""
    log.info(f"Writing parquet to {local_path}")
    df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)
    size_mb = local_path.stat().st_size / 1024 / 1024
    log.info(f"Parquet file size: {size_mb:.2f} MB")


def upload_to_gcs(local_path: Path, bucket_name: str, object_name: str) -> str:
    """Upload local file to GCS. Returns the gs:// URI."""
    log.info(f"Uploading to gs://{bucket_name}/{object_name}")
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))
    gcs_uri = f"gs://{bucket_name}/{object_name}"
    log.info(f"Uploaded to {gcs_uri}")
    return gcs_uri


def main():
    log.info("=" * 60)
    log.info("Taxi ingestion starting")
    log.info(f"Project: {PROJECT_ID}")
    log.info(f"Target bucket: {RAW_BUCKET}")
    log.info(f"Ingestion date: {INGESTION_DATE}")
    log.info("=" * 60)

    bq_client = bigquery.Client(project=PROJECT_ID)

    df = query_taxi_data(bq_client, ROW_LIMIT)
    df = clean_taxi_data(df)
    write_parquet(df, LOCAL_TEMP)
    gcs_uri = upload_to_gcs(LOCAL_TEMP, RAW_BUCKET, GCS_OBJECT_NAME)

    LOCAL_TEMP.unlink()
    log.info("Local temp file removed")

    log.info("=" * 60)
    log.info(f"Done. Final rows: {len(df):,}")
    log.info(f"Output: {gcs_uri}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()