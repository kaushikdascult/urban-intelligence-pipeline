"""urban — operational CLI for the urban-intelligence-pipeline.

A thin Click wrapper over the existing ingestion modules and scripts.
This repo is intentionally non-packaged ([tool.uv] package = false), so
there is no console-script entry-point. Invoke via the justfile:

    just urban --help
    just urban ingest --date 2022-01-15

or directly:

    uv run python -m cli --help

The ingestion modules expose main(ingestion_date: date | None = None).
This CLI calls them with an explicit date (the Airflow-style call path),
reusing their orchestration and never re-parsing argv or re-validating
dates — that logic stays in each module's parse_args, untouched.
"""

import click


@click.group()
@click.version_option("0.1.0", message="%(version)s")
def urban():
    """Operational tooling for the urban data pipeline."""


@urban.command()
@click.option(
    "--date",
    "ingestion_date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Ingestion date, YYYY-MM-DD.",
)
@click.option(
    "--skip-load",
    is_flag=True,
    help="Ingest taxi+weather to GCS but skip the BigQuery load.",
)
def ingest(ingestion_date, skip_load):
    """Ingest one day: taxi + weather -> GCS, then load -> BigQuery.

    Calls each module's main(ingestion_date=<date>) directly — the
    Airflow-style call path. parse_args/argv in those modules is never
    invoked, so their CLI contract (tested by test_ingestion_paths.py)
    is untouched. Date validation happens here, in Click.
    """
    # Click hands back a datetime; the modules type-hint date.
    day = ingestion_date.date()

    from ingestion.batch import taxi_ingestion, weather_ingestion

    click.echo(f"==> Taxi ingestion for {day.isoformat()}")
    taxi_ingestion.main(ingestion_date=day)

    click.echo(f"==> Weather ingestion for {day.isoformat()}")
    weather_ingestion.main(ingestion_date=day)

    if skip_load:
        click.echo("==> Skipping BigQuery load (--skip-load).")
        return

    from ingestion.batch import load_to_bigquery

    click.echo(f"==> Loading {day.isoformat()} into BigQuery")
    load_to_bigquery.main(ingestion_date=day)

    click.echo(f"==> Done: {day.isoformat()} ingested and loaded.")


@urban.command()
@click.option(
    "--start",
    "start_date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="First date (inclusive), YYYY-MM-DD.",
)
@click.option(
    "--end",
    "end_date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Last date (inclusive), YYYY-MM-DD.",
)
@click.option(
    "--skip-if-exists",
    is_flag=True,
    help="Skip dates whose GCS partition already exists (resumable).",
)
@click.option(
    "--skip-load",
    is_flag=True,
    help="Ingest to GCS only; skip the BigQuery load step.",
)
def backfill(start_date, end_date, skip_if_exists, skip_load):
    """Backfill a date range. Wraps scripts/backfill.py.

    Implemented in commit 4.3.
    """
    raise click.ClickException("urban backfill is implemented in commit 4.3.")


@urban.command()
@click.option(
    "--since",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Report BigQuery spend since this date, YYYY-MM-DD.",
)
def cost(since):
    """Report BigQuery bytes-billed cost since a date.

    Implemented in commit 4.4.
    """
    raise click.ClickException("urban cost is implemented in commit 4.4.")


if __name__ == "__main__":
    urban()
