"""
Backfill historical date ranges through the ingestion + load pipeline.

Replaces the ad-hoc shell loops used during initial backfill with a
single resumable script. Operational design:

  - Idempotent: re-running a date overwrites GCS files and DELETE-APPENDs
    the BQ partition. Safe to interrupt and restart.
  - Resumable: --start-date / --end-date define an inclusive range.
  - Skip-existing: --skip-if-exists checks GCS before each ingest step.
  - Fail-fast: any subprocess failure stops the loop with a non-zero
    exit code (unlike the shell `for` loop, which swallowed 30 schema
    errors during the Jan 2022 backfill).
  - Throttled: --sleep between iterations to respect Open-Meteo's free
    tier.

Usage:
    python scripts/backfill.py --start-date 2022-01-01 --end-date 2022-01-31
    python scripts/backfill.py --start-date 2022-01-01 --end-date 2022-01-31 \\
        --skip-if-exists --sleep 1.0
    python scripts/backfill.py --start-date 2022-01-01 --end-date 2022-01-07 \\
        --skip-load   # ingest GCS only, skip BQ load (e.g. for cost control)
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from google.cloud import storage

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "urban-pipeline-kd-2026")
RAW_BUCKET = os.getenv("RAW_BUCKET", "urban-pipeline-kd-2026-raw")
REPO_ROOT = Path(__file__).resolve().parents[1]

TAXI_SCRIPT = REPO_ROOT / "ingestion" / "batch" / "taxi_ingestion.py"
WEATHER_SCRIPT = REPO_ROOT / "ingestion" / "batch" / "weather_ingestion.py"
LOAD_SCRIPT = REPO_ROOT / "ingestion" / "batch" / "load_to_bigquery.py"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    iso = lambda s: datetime.strptime(s, "%Y-%m-%d").date()
    p.add_argument("--start-date", type=iso, required=True)
    p.add_argument("--end-date", type=iso, required=True)
    p.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="Skip ingest for dates whose GCS partition already exists.",
    )
    p.add_argument(
        "--skip-load",
        action="store_true",
        help="Run only the GCS ingestion steps; skip BigQuery load.",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds between iterations (Open-Meteo rate-limit hygiene).",
    )
    return p.parse_args()


def date_range(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def gcs_partition_exists(bucket: storage.Bucket, prefix: str) -> bool:
    """True if at least one blob exists under the given prefix."""
    return any(True for _ in bucket.list_blobs(prefix=prefix, max_results=1))


def run_step(label: str, cmd: list[str]) -> None:
    """Run a subprocess, stream output, raise on non-zero exit."""
    log.info(f"  → {label}: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed with exit code {result.returncode}")


def main() -> int:
    args = parse_args()
    if args.start_date > args.end_date:
        log.error("--start-date must be <= --end-date")
        return 2

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(RAW_BUCKET)

    total_days = (args.end_date - args.start_date).days + 1
    log.info("=" * 60)
    log.info(f"Backfill: {args.start_date} → {args.end_date} ({total_days} days)")
    log.info(f"Skip-if-exists: {args.skip_if_exists}   Skip-load: {args.skip_load}")
    log.info("=" * 60)

    for d in date_range(args.start_date, args.end_date):
        iso = d.isoformat()
        log.info(f"\n=== {iso} ===")

        taxi_prefix = f"taxi/ingestion_date={iso}/"
        weather_prefix = f"weather/ingestion_date={iso}/"

        # --- Taxi ingest ---
        if args.skip_if_exists and gcs_partition_exists(bucket, taxi_prefix):
            log.info(f"  taxi GCS exists, skipping ingest")
        else:
            run_step(
                "taxi ingest",
                [sys.executable, str(TAXI_SCRIPT), "--ingestion-date", iso],
            )

        # --- Weather ingest ---
        if args.skip_if_exists and gcs_partition_exists(bucket, weather_prefix):
            log.info(f"  weather GCS exists, skipping ingest")
        else:
            run_step(
                "weather ingest",
                [sys.executable, str(WEATHER_SCRIPT), "--ingestion-date", iso],
            )

        # --- BQ load (taxi + weather, single call) ---
        if not args.skip_load:
            run_step(
                "bq load",
                [sys.executable, str(LOAD_SCRIPT), "--ingestion-date", iso],
            )

        time.sleep(args.sleep)

    log.info("=" * 60)
    log.info(f"Backfill complete: {total_days} days processed.")
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
