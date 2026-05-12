"""
Tests for ingestion GCS path conventions and CLI contracts.

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

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

INGESTION_SCRIPTS = {
    "taxi": REPO_ROOT / "ingestion" / "batch" / "taxi_ingestion.py",
    "weather": REPO_ROOT / "ingestion" / "batch" / "weather_ingestion.py",
    "load_to_bq": REPO_ROOT / "ingestion" / "batch" / "load_to_bigquery.py",
}

HIVE_DATE_PATTERN = re.compile(r"ingestion_date=\d{4}-\d{2}-\d{2}")


def run_script(script: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(script), *args],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("name,script", INGESTION_SCRIPTS.items())
def test_script_accepts_ingestion_date_flag(name, script):
    """--help should list --ingestion-date."""
    result = run_script(script, "--help")
    out = result.stdout + result.stderr
    assert (
        "--ingestion-date" in out
    ), f"{name}: --ingestion-date missing from --help:\n{out}"


@pytest.mark.parametrize("name,script", INGESTION_SCRIPTS.items())
def test_script_rejects_malformed_date(name, script):
    """Invalid dates fail at arg-parse, before any GCP call."""
    result = run_script(script, "--ingestion-date", "2022-13-01")
    assert result.returncode != 0, f"{name}: should have rejected 2022-13-01"
    err = (result.stderr + result.stdout).lower()
    assert "invalid" in err or "error" in err


@pytest.mark.parametrize("name,script", INGESTION_SCRIPTS.items())
@pytest.mark.parametrize(
    "bad", ["01/15/2022", "2022/01/15", "15-01-2022", "Jan 15 2022"]
)
def test_script_rejects_non_iso_date(name, script, bad):
    """Common date typos (slashes, US format) must fail."""
    result = run_script(script, "--ingestion-date", bad)
    assert result.returncode != 0, f"{name}: should have rejected: {bad}"


def test_taxi_gcs_path_convention_in_source():
    """Taxi script must produce Hive-style paths to taxi_trips.parquet."""
    source = INGESTION_SCRIPTS["taxi"].read_text(encoding="utf-8")
    assert "ingestion_date=" in source
    assert "taxi_trips.parquet" in source


def test_weather_gcs_path_convention_in_source():
    """Weather script must produce Hive-style paths to weather_nyc.json."""
    source = INGESTION_SCRIPTS["weather"].read_text(encoding="utf-8")
    assert "ingestion_date=" in source
    assert "weather_nyc.json" in source


def test_load_to_bq_reads_hive_paths():
    """load_to_bigquery must reference both source paths and the reshaped path."""
    source = INGESTION_SCRIPTS["load_to_bq"].read_text(encoding="utf-8")
    assert "taxi/ingestion_date=" in source
    assert "weather/ingestion_date=" in source
    assert "weather/_reshaped/ingestion_date=" in source
