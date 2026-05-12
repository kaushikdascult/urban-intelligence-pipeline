"""
Weather data ingestion: Open-Meteo API → GCS as JSON.

Fetches hourly NYC weather for a single ingestion date (default: today, UTC)
to align with the taxi dataset. Lands in raw GCS bucket partitioned by
ingestion date.

Usage:
    python weather_ingestion.py                          # today (UTC)
    python weather_ingestion.py --ingestion-date 2022-01-15
"""

import argparse
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from google.cloud import storage

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "urban-pipeline-kd-2026")
RAW_BUCKET = os.getenv("RAW_BUCKET", "urban-pipeline-kd-2026-raw")

# NYC coordinates
LATITUDE = 40.7128
LONGITUDE = -74.0060

# Open-Meteo historical archive endpoint (free, no API key required)
API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "wind_direction_10m",
    "weather_code",
    "surface_pressure",
    "cloud_cover",
]

LOCAL_TEMP = Path("temp_weather_nyc.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse CLI args. --ingestion-date defaults to today (UTC)."""
    parser = argparse.ArgumentParser(
        description="Ingest one day of NYC hourly weather to GCS.",
    )
    parser.add_argument(
        "--ingestion-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=datetime.now(timezone.utc).date(),
        help="ISO date (YYYY-MM-DD) to ingest. Defaults to today (UTC).",
    )
    return parser.parse_args()


def fetch_weather(ingestion_date: date) -> dict:
    """Call Open-Meteo archive API for a single day and return raw JSON."""
    iso = ingestion_date.isoformat()
    log.info(f"Fetching weather for NYC ({LATITUDE}, {LONGITUDE}) on {iso}")

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": iso,
        "end_date": iso,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "America/New_York",
    }

    response = requests.get(API_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    hours = len(data.get("hourly", {}).get("time", []))
    log.info(f"Received {hours} hourly records")
    return data


def enrich_metadata(data: dict, ingestion_date: date) -> dict:
    """Wrap API payload with ingestion metadata for downstream tracking."""
    iso = ingestion_date.isoformat()
    return {
        "ingestion_metadata": {
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_api": API_URL,
            "city": "New York",
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "ingestion_date": iso,
            "start_date": iso,
            "end_date": iso,
        },
        "data": data,
    }


def write_local_json(payload: dict, local_path: Path) -> None:
    """Write JSON to local file."""
    log.info(f"Writing JSON to {local_path}")
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    size_kb = local_path.stat().st_size / 1024
    log.info(f"JSON file size: {size_kb:.1f} KB")


def upload_to_gcs(local_path: Path, bucket_name: str, object_name: str) -> str:
    """Upload local file to GCS. Returns the gs:// URI."""
    log.info(f"Uploading to gs://{bucket_name}/{object_name}")
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path), content_type="application/json")
    gcs_uri = f"gs://{bucket_name}/{object_name}"
    log.info(f"Uploaded to {gcs_uri}")
    return gcs_uri


def main(ingestion_date: date | None = None):
    """Run ingestion for a given date.

    When called from CLI, ingestion_date is None and we parse argv.
    When called from Airflow, the caller passes ingestion_date directly.
    """
    if ingestion_date is None:
        args = parse_args()
        ingestion_date = args.ingestion_date

    gcs_object_name = (
        f"weather/ingestion_date={ingestion_date.isoformat()}/weather_nyc.json"
    )

    log.info("=" * 60)
    log.info("Weather ingestion starting")
    log.info(f"Project: {PROJECT_ID}")
    log.info(f"Target bucket: {RAW_BUCKET}")
    log.info(f"Ingestion date: {ingestion_date.isoformat()}")
    log.info("=" * 60)

    raw = fetch_weather(ingestion_date)
    enriched = enrich_metadata(raw, ingestion_date)
    write_local_json(enriched, LOCAL_TEMP)
    gcs_uri = upload_to_gcs(LOCAL_TEMP, RAW_BUCKET, gcs_object_name)

    LOCAL_TEMP.unlink()
    log.info("Local temp file removed")

    log.info("=" * 60)
    log.info("Done.")
    log.info(f"Output: {gcs_uri}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
