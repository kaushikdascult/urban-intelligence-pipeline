"""
Tests for scripts/backfill.py CLI contract.

We don't actually invoke the backfill (it hits real GCS + BQ + Open-Meteo).
These tests just lock in the CLI surface area so it can't drift silently.
"""

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BACKFILL_SCRIPT = REPO_ROOT / "scripts" / "backfill.py"


def run_script(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(BACKFILL_SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def test_script_exists():
    assert BACKFILL_SCRIPT.exists(), f"Missing: {BACKFILL_SCRIPT}"


def test_script_accepts_required_flags():
    """--help should list both --start-date and --end-date."""
    result = run_script("--help")
    out = result.stdout + result.stderr
    assert "--start-date" in out
    assert "--end-date" in out
    assert "--skip-if-exists" in out


def test_script_requires_start_and_end_date():
    """Running with no args should fail clearly."""
    result = run_script()
    assert result.returncode != 0
    err = (result.stderr + result.stdout).lower()
    assert "start-date" in err or "required" in err


def test_script_rejects_inverted_range():
    """end-date before start-date should fail at argparse, not at runtime."""
    result = run_script("--start-date", "2022-01-31", "--end-date", "2022-01-01")
    assert result.returncode != 0
    err = (result.stderr + result.stdout).lower()
    assert "before" in err or "start-date" in err


@pytest.mark.parametrize("bad", ["01/15/2022", "Jan 15 2022", "2022-13-01"])
def test_script_rejects_malformed_dates(bad):
    """Malformed dates fail at arg-parse, before any GCP call."""
    result = run_script("--start-date", bad, "--end-date", "2022-01-31")
    assert result.returncode != 0
