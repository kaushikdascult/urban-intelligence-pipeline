"""
Backfill GCS partitions for a date range.

Loops over [start_date, end_date] inclusive and runs taxi + weather
ingestion for each day. Does NOT run load_to_bigquery or spark_transform
— those use WRITE_TRUNCATE / overwrite semantics and will be fixed
in Day 8. For now, run them manually once at the end against the
last date if you want BQ populated.

Idempotent: re-running overwrites the same GCS objects with identical
content. Resumable via --start-date / --end-date. Optional
--skip-if-exists checks GCS before each day and skips already-populated
partitions.

Usage:
    python scripts/backfill.py --start-date 2022-01-01 --end-date 2022-01-31
    python scripts/backfill.py --start-date 2022-01-15 --end-date 2022-01-31 --skip-if-exists
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# Make ingestion modules importable when run from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from google.cloud import storage  # noqa: E402

from ingestion.batch.taxi_ingestion import main as taxi_main  # noqa: E402
from ingestion.batch.weather_ingestion import main as weather_main  # noqa: E402

RAW_BUCKET = os.getenv("RAW_BUCKET", "urban-pipeline-kd-2026-raw")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "urban-pipeline-kd-2026")

# Open-Meteo asks for courteous use of their free archive API.
# 2s is well below any rate limit but keeps us polite.
WEATHER_SLEEP_SECONDS = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("backfill")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill GCS partitions for a date range (taxi + weather only).",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
        help="First date to backfill (inclusive, ISO format)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
        help="Last date to backfill (inclusive, ISO format)",
    )
    parser.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="Skip dates whose GCS partition already has both taxi and weather files",
    )
    args = parser.parse_args()

    if args.end_date < args.start_date:
        parser.error(
            f"--end-date {args.end_date} is before --start-date {args.start_date}"
        )

    return args


def daterange(start: date, end: date):
    """Yield each date in [start, end] inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def gcs_partition_exists(
    storage_client: storage.Client, ingestion_date: date
) -> tuple[bool, bool]:
    """Return (taxi_exists, weather_exists) for the given date's GCS partition."""
    iso = ingestion_date.isoformat()
    bucket = storage_client.bucket(RAW_BUCKET)

    taxi_blob = bucket.blob(f"taxi/ingestion_date={iso}/taxi_trips.parquet")
    weather_blob = bucket.blob(f"weather/ingestion_date={iso}/weather_nyc.json")

    return taxi_blob.exists(), weather_blob.exists()


def main():
    args = parse_args()
    start = args.start_date
    end = args.end_date
    days = (end - start).days + 1

    log.info("=" * 70)
    log.info("BACKFILL")
    log.info(f"Range:           {start.isoformat()} → {end.isoformat()} ({days} days)")
    log.info(f"Project:         {PROJECT_ID}")
    log.info(f"Target bucket:   {RAW_BUCKET}")
    log.info(f"Skip if exists:  {args.skip_if_exists}")
    log.info("=" * 70)

    storage_client = storage.Client(project=PROJECT_ID) if args.skip_if_exists else None

    started_at = time.monotonic()
    succeeded = 0
    skipped = 0
    failed: list[tuple[date, str]] = []

    for d in daterange(start, end):
        iso = d.isoformat()
        log.info("")
        log.info("-" * 70)
        log.info(f"[{succeeded + skipped + len(failed) + 1}/{days}] {iso}")
        log.info("-" * 70)

        if args.skip_if_exists:
            taxi_exists, weather_exists = gcs_partition_exists(storage_client, d)
            if taxi_exists and weather_exists:
                log.info(f"{iso}: both partitions exist, skipping")
                skipped += 1
                continue
            if taxi_exists:
                log.info(f"{iso}: taxi exists; will re-run weather only")
            if weather_exists:
                log.info(f"{iso}: weather exists; will re-run taxi only")

        try:
            taxi_main(ingestion_date=d)
        except Exception as exc:
            log.error(f"{iso}: taxi ingestion FAILED: {exc}")
            failed.append((d, f"taxi: {exc}"))
            continue

        time.sleep(WEATHER_SLEEP_SECONDS)

        try:
            weather_main(ingestion_date=d)
        except Exception as exc:
            log.error(f"{iso}: weather ingestion FAILED: {exc}")
            failed.append((d, f"weather: {exc}"))
            continue

        succeeded += 1

    elapsed = time.monotonic() - started_at

    log.info("")
    log.info("=" * 70)
    log.info("BACKFILL SUMMARY")
    log.info(f"Elapsed:     {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    log.info(f"Succeeded:   {succeeded}/{days}")
    log.info(f"Skipped:     {skipped}/{days}")
    log.info(f"Failed:      {len(failed)}/{days}")
    if failed:
        log.info("")
        log.info("Failures:")
        for d, reason in failed:
            log.info(f"  {d.isoformat()}: {reason}")
    log.info("=" * 70)

    log.info("")
    log.info("Next step: GCS is now populated. Until Day 8 fixes the WRITE_TRUNCATE")
    log.info("issue, you can manually load the last day into BQ with:")
    log.info(
        f"  python ingestion/batch/load_to_bigquery.py --ingestion-date {end.isoformat()}"
    )

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
