"""
Weather data ingestion: Open-Meteo API → GCS as JSON.

Fetches hourly NYC weather for January 2022 to align with the taxi dataset.
Lands in raw GCS bucket partitioned by ingestion date.
"""

import json
import logging
import os
from datetime import datetime, timezone
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
START_DATE = "2022-01-01"
END_DATE = "2022-01-31"

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

INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
GCS_OBJECT_NAME = f"weather/ingestion_date={INGESTION_DATE}/weather_nyc.json"

LOCAL_TEMP = Path("temp_weather_nyc.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def fetch_weather() -> dict:
    """Call Open-Meteo archive API and return raw JSON response."""
    log.info(f"Fetching weather for NYC ({LATITUDE}, {LONGITUDE})")
    log.info(f"Date range: {START_DATE} to {END_DATE}")

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "America/New_York",
    }

    response = requests.get(API_URL, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    hours = len(data.get("hourly", {}).get("time", []))
    log.info(f"Received {hours} hourly records")
    return data


def enrich_metadata(data: dict) -> dict:
    """Wrap API payload with ingestion metadata for downstream tracking."""
    return {
        "ingestion_metadata": {
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_api": API_URL,
            "city": "New York",
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": START_DATE,
            "end_date": END_DATE,
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


def main():
    log.info("=" * 60)
    log.info("Weather ingestion starting")
    log.info(f"Project: {PROJECT_ID}")
    log.info(f"Target bucket: {RAW_BUCKET}")
    log.info(f"Ingestion date: {INGESTION_DATE}")
    log.info("=" * 60)

    raw = fetch_weather()
    enriched = enrich_metadata(raw)
    write_local_json(enriched, LOCAL_TEMP)
    gcs_uri = upload_to_gcs(LOCAL_TEMP, RAW_BUCKET, GCS_OBJECT_NAME)

    LOCAL_TEMP.unlink()
    log.info("Local temp file removed")

    log.info("=" * 60)
    log.info("Done.")
    log.info(f"Output: {gcs_uri}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
