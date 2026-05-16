### Airflow DAG — Task Graph
# ![Pipeline](docs/airflow_pipeline.png)

# Urban Intelligence Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.10-017CEE?style=flat&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4?style=flat&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=flat&logo=terraform&logoColor=white)](https://www.terraform.io)
[![CI](https://github.com/kaushikdascult/urban-intelligence-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/kaushikdascult/urban-intelligence-pipeline/actions/workflows/ci.yml)

> Production-grade batch data engineering pipeline on GCP — ingests NYC taxi trip data and hourly weather, transforms with PySpark on Dataproc Serverless, models with dbt, orchestrates with Airflow.

## Project overview

This is a portfolio project demonstrating production data engineering practices on Google Cloud Platform. It ingests **~2.43M** NYC Yellow Taxi trips alongside **744** hourly weather records for January 2022 (31 daily partitions), joins and enriches them via PySpark on Dataproc Serverless, models them as a star schema with dbt, and orchestrates the entire pipeline end-to-end with Apache Airflow.

**Key question explored:** *How does NYC weather affect taxi trip patterns and fares?*

**Sample finding:**
The full January 2022 backfill (~2.43M taxi trips) makes the real Jan 28–29 Nor'easter visible: Jan 28 saw 95,297 trips, Jan 29 collapsed to 34,195 (a 64% drop), with Jan 30 partially recovering to 70,805. The pre-Day-7 "LIMIT 500000" query sampled the month uniformly and masked this signal.

## Architecture

~~~
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│   BigQuery Public Dataset           Open-Meteo API (Free)        │
│   NYC Yellow Taxi 2022              Hourly NYC Weather 2022      │
│   ~2.43M records (31 partitions)    744 hourly records           │
└──────────────┬───────────────────────────────┬──────────────────┘
               │                               │
               ▼                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                       INGESTION LAYER                            │
│   taxi_ingestion.py        weather_ingestion.py                  │
│   load_to_bigquery.py                                            │
│              GCS Raw Bucket (Parquet + JSON)                     │
│         partitioned by ingestion_date (Hive-style)               │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER                          │
│         PySpark on GCP Dataproc Serverless (zero idle cost)      │
│   spark_transform.py — join taxi+weather + feature engineering   │
│   DELETE-then-APPEND per partition (pickup_date),                │
│   clustered by (pickup_hour, pickup_location_id)                 │
│         ~2.43M enriched rows across 31 daily partitions          │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      MODELLING LAYER (dbt)                       │
│   stg_trips (view) → fct_trips (incremental, partitioned)        │
│   dim_dates · dim_locations · dim_locations SCD2 snapshot        │
│        14 data quality tests · BigQuery marts (star schema)      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION LAYER (Airflow)                   │
│    6-task DAG · @daily schedule · parallel ingestion             │
│    Docker locally · Composer-ready for cloud                     │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
                     Analysis-ready marts
~~~

## Tech stack

| Layer            | Technology                  | Detail                                                    |
|------------------|-----------------------------|-----------------------------------------------------------|
| Infrastructure   | Terraform 1.5+              | 24 GCP resources via IaC, GCS-backed remote state         |
| Storage          | GCS (3 buckets)             | Raw, staging, scripts; lifecycle 90d, public access blocked |
| Data warehouse   | BigQuery                    | Raw, staging, marts datasets — partitioned + clustered    |
| Ingestion        | Python 3.11 + GCS/BQ SDK    | Public BigQuery + Open-Meteo REST API → Parquet/JSON       |
| Transformation   | PySpark 3.5 on Dataproc Serverless | Join + feature engineering; scales to zero          |
| Modelling        | dbt-bigquery 1.8            | Star schema · incremental fct_trips · SCD2 snapshot · 14 tests |
| Orchestration    | Apache Airflow 2.10         | 6-task DAG via Docker (LocalExecutor)                     |
| Auth             | OAuth + ADC                 | No service-account keys (org policy compliant)            |
| CI/CD            | GitHub Actions + uv         | DAG syntax · dbt parse · pre-commit (black/flake8/hygiene) |
| Dev tooling      | uv · just · pre-commit      | Locked deps (uv.lock) · task runner · git hooks           |
| Languages        | Python · SQL · HCL · YAML   |                                                           |

## Project structure

~~~
urban-intelligence-pipeline/
├── .github/workflows/             # CI: DAG validation · dbt parse · pre-commit
├── pyproject.toml                 # deps + tool config (uv-managed, uv.lock pinned)
├── justfile                       # task runner: setup/test/lint/transform/...
├── .pre-commit-config.yaml        # black · flake8 · hygiene hooks
├── .flake8                        # flake8 config (line-length 88, black-compatible)
├── .gitattributes                 # enforce LF line endings repo-wide
├── terraform/                     # IaC — 24 GCP resources
│   ├── apis.tf                    # 10 enabled APIs
│   ├── gcs.tf                     # 3 GCS buckets with lifecycle
│   ├── bigquery.tf                # 3 BigQuery datasets
│   ├── iam.tf                     # SA + 8 IAM bindings
│   └── ...
├── ingestion/batch/               # Python: extract → GCS → BQ raw
│   ├── taxi_ingestion.py          # parameterized by --ingestion-date
│   ├── weather_ingestion.py       # 24 hourly records per day
│   └── load_to_bigquery.py        # DELETE-then-APPEND per partition
├── scripts/
│   ├── backfill.py                # resumable ingestion backfill
│   └── transform_backfill.sh      # resumable Dataproc batch backfill (silver layer)
├── transform/
│   └── spark_transform.py         # Dataproc Serverless PySpark job
├── dbt/urban_pipeline_dbt/
│   ├── models/staging/            # stg_trips with surrogate keys
│   ├── models/marts/              # dim/fact star schema
│   ├── snapshots/                 # dim_locations SCD2 snapshot
│   ├── tests/                     # 3 singular tests (positive_fares, etc.)
│   └── analyses/                  # data_freshness, weather_impact_summary
├── airflow/
│   ├── docker-compose.yml         # Postgres + scheduler + webserver
│   └── dags/urban_pipeline_dag.py # 6-task DAG
├── tests/                         # pytest smoke tests
├── docs/
│   └── ADR-001-platform-choices.md  # Architecture Decision Record
└── README.md
~~~

## Data layers

| Layer | Format | Location | Rows (Jan 2022) |
| --- | --- | --- | --- |
| Raw — taxi | parquet | `gs://…-raw/taxi/ingestion_date=…/` | 31 partitions, ~2.43M |
| Raw — weather | json | `gs://…-raw/weather/ingestion_date=…/` | 31 partitions, 744 hours |
| Raw — BQ | partitioned table | `raw.taxi_trips`, `raw.weather_hourly` | matches GCS |
| Staging | partitioned + clustered | `staging.taxi_trips_enriched` | 31 partitions, ~2.45M |
| Marts | partitioned, incremental | `marts.fct_trips` | ~2.4M (quality-filtered) |
| Marts — snapshot | SCD2 | `snapshots.dim_locations_snapshot` | 261 zones |

## Build log

### Day 1 — Terraform foundation

Provisioned 24 GCP resources via Terraform: 10 APIs enabled, 3 GCS buckets (raw/staging/scripts) with uniform bucket-level access and 90-day lifecycle, 3 BigQuery datasets, dedicated service account `urban-pipeline-sa` with 8 IAM roles, and remote state in `gs://urban-pipeline-kd-2026-tf-state`.

### Day 2 — Python ingestion

Built three ingestion scripts: taxi pull from BigQuery public dataset (`bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`) — originally cleaned to a 485K-row sample, later parameterized in Day 7 to load full daily partitions (~2.43M rows across 31 days for January 2022); Open-Meteo API call producing 24 hourly records per day; parquet/JSON loaders into BQ raw.

### Day 3 — PySpark on Dataproc Serverless

Wrote `spark_transform.py` that reads BQ raw, joins on `date_trunc('hour', pickup_datetime) == to_timestamp(time)`, derives `is_raining`, `is_snowing`, `weather_severity` (clear/freezing/light_rain/heavy_rain/blizzard), `tip_pct`. Submitted as Dataproc Serverless batch with `writeMethod=indirect` to preserve partitioning. Output: 484K rows initially (sampled raw), later rebuilt across 31 daily partitions on Day 7's full backfill.

### Day 4 — dbt models

Set up dbt-bigquery with OAuth via ADC. Built `stg_trips` (view), `dim_dates` (date spine 2022-01-01 to 2022-12-31), `dim_locations` (260 distinct pickup+dropoff zones), and `fct_trips` (partitioned by `pickup_date`, clustered by `pickup_hour, pickup_location_id` — converted to incremental materialization in Day 8). All 11 generic tests passing.

### Day 5 — Airflow orchestration

Built 6-task DAG running ingestion → load → Spark transform → dbt run → dbt test. Deployed via Docker Compose (postgres + scheduler + webserver) using LocalExecutor. Worked around org-level service-account key restriction by mounting ADC credentials as a Docker volume.

### Day 6 — Tests, monitoring, README

Added pytest smoke tests for ingestion modules. Added 3 singular dbt tests (`positive_fares`, `no_future_trips`, `dropoff_after_pickup`). Added monitoring SQL analyses (`data_freshness`, `weather_impact_summary`). Created GitHub Actions CI workflow.

### Day 7 — Incremental ingestion + full backfill

Parameterized `taxi_ingestion.py`, `weather_ingestion.py`, and `load_to_bigquery.py` with `--ingestion-date`. Switched `load_to_bigquery.py` from `WRITE_TRUNCATE` to DELETE-then-APPEND scoped to the ingestion date so daily runs are idempotent and safe to retry. Wired the Airflow DAG to pass `{{ ds }}` (logical date) through to every task. Added `scripts/backfill.py` for resumable date-range backfills with `--skip-if-exists` and `--skip-load` flags. Backfilled full January 2022 (~2.43M taxi rows + 744 weather rows across 31 daily partitions).

### Day 8 — Partition-aware layers: incremental models + SCD2

Made every layer respect the partition instead of rebuilding the world.
Refactored `spark_transform.py` to do per-partition DELETE-then-APPEND into
`staging.taxi_trips_enriched` (predicate pushdown on read, `--ingestion-date`
scoped DELETE before the INDIRECT-mode append), and backfilled all 31 days
via `scripts/transform_backfill.sh`. Hit the trial-tier 4-vCPU
`CPUS_ALL_REGIONS` quota — a single Dataproc Serverless batch needs 12 —
diagnosed it from the batch `stateMessage`, raised the quota to 24, and
rebuilt the backfill loop to submit synchronously so the quota paces the
loop instead of breaking it. Converted `fct_trips` from a full-rebuild
`table` to an `incremental` model with `insert_overwrite` (full refresh
processes ~399 MiB; an incremental run processes ~37 MiB). Added an SCD2
snapshot for `dim_locations` using dbt's `check` strategy. Repaired type
mismatches across `stg_trips` and the dimension joins left over from the
Day 7 raw-schema correction.

### Day 9 — Packaging & developer experience

No new data work — made the repo reproducible and contribution-ready.
Replaced the bare `requirements.txt` with `pyproject.toml` + `uv`, with
exact versions pinned in a committed `uv.lock` so `uv sync` is
deterministic and "works on my machine" drift is impossible. Added a
`justfile` task runner so the commands that were scattered through this
README (`setup`, `lint`, `test`, `transform`, `dbt-run`, full local `ci`)
are one word each. Added `pre-commit` hooks (black, flake8, plus hygiene)
that fire on every commit, and a `.flake8` config setting line-length to
88 so flake8 stops fighting black — this surfaced 88 latent lint
violations the old `continue-on-error: true` CI had been silently
swallowing, all since fixed. Migrated three of four CI jobs from
`pip install` to `uv sync` so `pyproject.toml` is the single source of
truth, and swapped the ad-hoc black/flake8 CI steps for
`pre-commit run --all-files` so CI and local enforce identical rules.
Pinned all CI actions to immutable Node-24 tags after the Node-20
deprecation notice (a two-pass fix: the first bump landed on tags that
were themselves still Node-20). Added `.gitattributes` to stop Windows
`autocrlf` from fighting the end-of-file hook on every checkout.

## Key engineering decisions

- **Dataproc Serverless over cluster**: zero idle cost, scales to zero between runs.
- **Indirect write mode + `createDisposition=CREATE_IF_NEEDED`**: BQ Spark connector silently dropped partitioning config in direct mode.
- **OAuth/ADC over service account keys**: org policy `iam.disableServiceAccountKeyCreation` enforced; ADC volume-mounted into Airflow containers.
- **Hive-style date partitioning in raw bucket**: `taxi/ingestion_date=YYYY-MM-DD/` enables efficient time-range scans.
- **DELETE-then-APPEND per partition**: `load_to_bigquery.py` deletes the target partition before appending, so re-running a date is idempotent and partial backfills resume cleanly.
- **Surrogate keys via wide MD5**: `MD5(pickup_dt, dropoff_dt, pickup_loc, dropoff_loc, fare, tip, distance)` — narrower hashes produced ~36 collisions.
- **Cluster on `pickup_hour, pickup_location_id`**: matches the most common analytical query pattern (hourly breakdowns by zone).
- **`insert_overwrite` for incremental `fct_trips`**: atomically replaces whole partitions rather than row-level MERGE — cheaper, idempotent, and avoids depending on the MD5 surrogate key for a merge key.
- **`check` strategy for the `dim_locations` snapshot**: the dimension is derived from trip-volume aggregates and has no natural `updated_at` column, so dbt diffs the watched columns directly.

See [`docs/ADR-001-platform-choices.md`](docs/ADR-001-platform-choices.md) for a full architecture decision record.

## Tests

~~~bash
# Python smoke tests (module imports, constants, file structure, path conventions)
just test

# dbt tests (11 generic + 3 singular)
just dbt-test

# dbt SCD2 snapshot — capture dim_locations history
cd dbt/urban_pipeline_dbt && uv run dbt snapshot --profiles-dir ~/.dbt
~~~

CI runs the full suite (`just ci` equivalent) on every push to `main`.

## Cost

Approximate cost for one full January 2022 backfill on GCP:

| Component | Cost |
| --- | --- |
| GCS storage (raw bucket, ~50 MB across 31 partitions) | <$0.01 |
| BigQuery storage (post-Day 8: ~500 MB physical) | <$0.01 |
| BQ public-dataset queries (31 × ~30 MB partition scan) | $0 (covered by user's free tier) |
| Dataproc Serverless batch (one run per backfill) | ~$0.10 |
| dbt run on BQ (post-Day 8: incremental fct_trips) | ~$0.01 |
| **Total for full January 2022 backfill** | **~$0.12** |

Per-day incremental runs (Day 8+) will be substantially cheaper since dbt processes only new partitions.

## Sample analytical query

```sql
-- Trips per day across the Jan 28-29 Nor'easter
SELECT pickup_date, COUNT(*) AS trips
FROM `urban-pipeline-kd-2026.raw.taxi_trips`
WHERE pickup_date BETWEEN '2022-01-28' AND '2022-01-30'
GROUP BY pickup_date ORDER BY pickup_date;
-- 2022-01-28  95,297   (Friday, pre-storm)
-- 2022-01-29  34,195   (Saturday, Nor'easter — 64% collapse)
-- 2022-01-30  70,805   (Sunday, partial recovery)
```

The January 28–29 Nor'easter is visible in the raw layer: Friday Jan 28 saw 95,297 trips (a normal weekday volume), Saturday Jan 29 collapsed to 34,195 (a 64% drop, less than half the surrounding Saturdays), and Sunday Jan 30 partially recovered to 70,805 as the city dug out. The same query runs against `marts.fct_trips` (swap the table name) — the full backfill flows through every layer.

## Running locally

### Prerequisites

- gcloud CLI authenticated (`gcloud auth application-default login`)
- Python 3.11 (`uv` manages the environment — install `uv` + `just` per Setup step 0)
- Terraform >= 1.5
- Docker Desktop (for Airflow)
- A GCP project with billing enabled

### Setup

~~~bash
# 0. One-time tool install (skip if you already have uv + just)
curl -LsSf https://astral.sh/uv/install.sh | sh   # uv
uv tool install rust-just                          # just

# 1. Set up GCP project + remote state bucket
gcloud projects create urban-pipeline-kd-2026
gcloud storage buckets create gs://urban-pipeline-kd-2026-tf-state --location=US

# 2. Provision infrastructure
cd terraform
cp terraform.tfvars.example terraform.tfvars  # edit your project ID
terraform init && terraform apply
cd ..

# 3. Python environment — installs locked deps into .venv and wires up
#    git pre-commit hooks (one command, replaces venv + pip install)
just setup

# 4. Run the pipeline for a single day
just transform              # uploads Spark script + runs the backfill script
cd dbt/urban_pipeline_dbt && uv run dbt build --profiles-dir ~/.dbt && cd ../..

# 5. OR run end-to-end via Airflow
cd airflow
echo "AIRFLOW_UID=50000" > .env
docker compose up airflow-init
docker compose up -d
# Open http://localhost:8080, login admin/admin, trigger urban_pipeline DAG
cd ..
~~~

Run `just` with no arguments to see every available task (setup, test,
lint, format, dbt-run, dbt-test, transform, ci, ...).

<details>
<summary>Raw commands without <code>just</code> (what the recipes wrap)</summary>

~~~bash
# Python env (equivalent to `just setup`)
uv sync --extra dev
uv run pre-commit install

# Single-day ingestion (no just recipe — parameterized by date)
uv run python ingestion/batch/taxi_ingestion.py    --ingestion-date 2022-01-31
uv run python ingestion/batch/weather_ingestion.py --ingestion-date 2022-01-31
uv run python ingestion/batch/load_to_bigquery.py  --ingestion-date 2022-01-31

# Spark transform (equivalent to `just transform`)
gcloud storage cp transform/spark_transform.py \
    gs://urban-pipeline-kd-2026-scripts/transform/
gcloud dataproc batches submit pyspark \
    gs://urban-pipeline-kd-2026-scripts/transform/spark_transform.py \
    --batch=urban-pipeline-$(date +%s) --region=us-central1 \
    --service-account=urban-pipeline-sa@urban-pipeline-kd-2026.iam.gserviceaccount.com \
    --version=2.2 --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar \
    -- --project=urban-pipeline-kd-2026 --raw-dataset=raw \
       --staging-dataset=staging --gcs-temp-bucket=urban-pipeline-kd-2026-staging \
       --ingestion-date=2022-01-31

# dbt
cd dbt/urban_pipeline_dbt && uv run dbt build --profiles-dir ~/.dbt
~~~

</details>

### Backfill historical dates

The DAG ingests one day at a time via Airflow's `{{ ds }}` macro (logical date / `data_interval_start`). For one-off historical loads — for example, populating January 2022 to demo the pipeline — use the standalone backfill script:

~~~bash
# Populate 31 GCS partitions + load to BQ
python scripts/backfill.py --start-date 2022-01-01 --end-date 2022-01-31

# Resume after a failure — skips dates whose GCS partition already exists
python scripts/backfill.py --start-date 2022-01-01 --end-date 2022-01-31 \
    --skip-if-exists

# Ingest GCS only, skip BQ load (e.g. for cost control)
python scripts/backfill.py --start-date 2022-01-01 --end-date 2022-01-31 \
    --skip-load
~~~

`scripts/backfill.py` calls `taxi_ingestion.py` → `weather_ingestion.py` → `load_to_bigquery.py` per day. The load step does DELETE-then-APPEND scoped to `--ingestion-date`, so running the same day twice is idempotent and partial backfills are safe to resume.

## Contributing

The repo is set up so a fresh clone is productive in two commands.

~~~bash
just setup     # uv sync --extra dev  +  pre-commit install
just ci        # everything CI runs: lint + tests + dbt compile
~~~

**Dependency management — `uv`.** Dependencies live in `pyproject.toml`;
exact resolved versions are pinned in `uv.lock` (committed). `uv sync`
reproduces the environment deterministically. Add a dependency with
`uv add <pkg>` (runtime) or `uv add --dev <pkg>` (tooling), which updates
both files. There is no `requirements.txt`.

**Task runner — `just`.** `just` with no arguments lists every task.
Recipes wrap the commands that otherwise live in scattered docs:
`just lint`, `just format`, `just test`, `just dbt-run`, `just transform`,
`just ci`. Each recipe runs in its own shell, which sidesteps the
Git-Bash-on-Windows quoting and `cd`-leak issues this project hit early on.

**Git hooks — `pre-commit`.** `just setup` installs hooks that run on
every commit: `black` (format), `flake8` (lint, configured in `.flake8`
to line-length 88 to agree with black), plus hygiene checks
(trailing whitespace, end-of-file, YAML/JSON validity, large-file guard,
merge-conflict markers). If a formatting hook rewrites a file, the commit
aborts by design — re-`git add` and commit again.

**CI parity.** The `code-quality` CI job runs `pre-commit run --all-files`,
so CI enforces exactly the same rules as the local hooks — no
"passes locally, fails in CI" drift. `just ci` reproduces the full CI
suite locally before you push.

**Action pinning.** CI actions are pinned to immutable version tags
(e.g. `astral-sh/setup-uv@v8.1.0`) rather than floating majors (`@v8`).
Floating major tags are the attack surface exploited in the `tj-actions`
supply-chain compromise; `setup-uv` no longer publishes floating major
tags for this reason.

## Architecture Decision Record

See [`docs/ADR-001-platform-choices.md`](docs/ADR-001-platform-choices.md) for the rationale on Dataproc Serverless vs cluster, OAuth/ADC vs service account keys, and Docker Airflow vs Cloud Composer.

## Author

**Kaushik Das**
Data Engineer · GCP · BigQuery · dbt · Airflow

[GitHub](https://github.com/kaushikdascult)

## License

Released under the MIT License. See [`LICENSE`](LICENSE) for the full text.
