# Urban Intelligence Pipeline — task runner
#
# Run `just` (no args) to list available tasks.
# All recipes assume you've installed `just` (uv tool install rust-just)
# and have `uv` available on PATH.

# Show available recipes when called without arguments
default:
    @just --list

# ---------------------------------------------------------------------------
# Setup & environment
# ---------------------------------------------------------------------------

# Install all dependencies (runtime + dev) into .venv via uv
setup:
    uv sync --extra dev
    uv run pre-commit install

# Print resolved versions of key tools (sanity check after `just setup`)
versions:
    uv run python --version
    uv run dbt --version
    uv run black --version
    uv run flake8 --version

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

# Run black --check + flake8. Matches what CI runs.
lint:
    uv run black --check --diff ingestion/ transform/ tests/ airflow/dags/ scripts/
    uv run flake8 ingestion/ transform/ tests/ airflow/dags/ scripts/

# Auto-format code in place with black (NOT what CI does — CI only checks).
format:
    uv run black ingestion/ transform/ tests/ airflow/dags/ scripts/

# Run all pre-commit hooks on every file (not just staged changes)
precommit:
    uv run pre-commit run --all-files

# Run the pytest smoke suite
test:
    uv run pytest tests/ -v

# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------
# All dbt commands run from the project subdirectory (Day-7 gotcha).

# Run dbt models (incremental where defined)
dbt-run:
    cd dbt/urban_pipeline_dbt && uv run dbt run

# Run all dbt tests
dbt-test:
    cd dbt/urban_pipeline_dbt && uv run dbt test

# Compile dbt without executing (cheap sanity check)
dbt-compile:
    cd dbt/urban_pipeline_dbt && uv run dbt compile

# ---------------------------------------------------------------------------
# Spark / Dataproc
# ---------------------------------------------------------------------------

# Re-upload the Spark transform script to GCS (Day-7 carry-forward gotcha)
upload-spark:
    gcloud storage cp transform/spark_transform.py \
        gs://urban-pipeline-kd-2026-scripts/transform/

# Run the 31-day silver-layer backfill (synchronous, quota-paced)
backfill:
    bash scripts/transform_backfill.sh

# Upload Spark script then run backfill (chains upload-spark + backfill)
transform: upload-spark backfill

# ---------------------------------------------------------------------------
# CI parity
# ---------------------------------------------------------------------------

# Run everything CI runs locally, in the same order. Use before pushing.
ci: lint test dbt-compile

# ---------------------------------------------------------------------------
# Operational CLI
# ---------------------------------------------------------------------------

# Run the urban operational CLI (wraps ingestion + backfill + cost reporting)
urban *ARGS:
    uv run python -m cli {{ARGS}}
