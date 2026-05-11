"""
Tests for ingestion GCS path conventions.

The raw bucket uses Hive-style date partitioning:
    taxi/ingestion_date=YYYY-MM-DD/taxi_trips.parquet
    weather/ingestion_date=YYYY-MM-DD/weather_nyc.json

Downstream Spark and dbt rely on this layout. These tests fail loud
if the convention drifts.
"""

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TAXI_SCRIPT = REPO_ROOT / "ingestion" / "batch" / "taxi_ingestion.py"

HIVE_DATE_PATTERN = re.compile(
    r"taxi/ingestion_date=\d{4}-\d{2}-\d{2}/taxi_trips\.parquet"
)


def run_script(*args: str) -> str:
    """Run the taxi ingestion script with --help or other safe flags."""
    result = subprocess.run(
        [sys.executable, str(TAXI_SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout + result.stderr


def test_taxi_script_accepts_ingestion_date_flag():
    """--help should list --ingestion-date."""
    out = run_script("--help")
    assert "--ingestion-date" in out, f"--ingestion-date missing from --help:\n{out}"


def test_taxi_script_rejects_malformed_date():
    """Invalid dates should fail at arg-parse, before any GCP call."""
    result = subprocess.run(
        [sys.executable, str(TAXI_SCRIPT), "--ingestion-date", "2022-13-01"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "invalid" in result.stderr.lower() or "error" in result.stderr.lower()


def test_taxi_script_rejects_non_iso_date():
    """Common date typos (slash separators, US format) must fail."""
    for bad in ["01/15/2022", "2022/01/15", "15-01-2022", "Jan 15 2022"]:
        result = subprocess.run(
            [sys.executable, str(TAXI_SCRIPT), "--ingestion-date", bad],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode != 0, f"Should have rejected: {bad}"


def test_gcs_path_convention_in_source():
    """
    The script must construct paths in Hive-style format:
        taxi/ingestion_date=YYYY-MM-DD/taxi_trips.parquet

    Downstream Spark partition pruning depends on this exact layout.
    """
    source = TAXI_SCRIPT.read_text(encoding="utf-8")
    assert (
        "ingestion_date=" in source
    ), "Hive-style 'ingestion_date=' prefix missing from taxi_ingestion.py"
    assert (
        "taxi_trips.parquet" in source
    ), "Output filename 'taxi_trips.parquet' missing"
