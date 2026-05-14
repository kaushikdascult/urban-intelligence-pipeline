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
| CI/CD            | GitHub Actions              | DAG syntax · dbt parse · Python lint                      |
| Languages        | Python · SQL · HCL · YAML   |                                                           |

## Project structure

~~~
urban-intelligence-pipeline/
├── .github/workflows/             # CI: DAG validation, dbt parse, Python lint
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
pytest tests/ -v

# dbt tests (11 generic + 3 singular)
cd dbt/urban_pipeline_dbt && dbt test --profiles-dir ~/.dbt

# dbt SCD2 snapshot — capture dim_locations history
cd dbt/urban_pipeline_dbt && dbt snapshot --profiles-dir ~/.dbt
~~~

CI runs both on every push to `main`.

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
- Python 3.11 with venv
- Terraform >= 1.5
- Docker Desktop (for Airflow)
- A GCP project with billing enabled

### Setup

~~~bash
# 1. Set up GCP project + remote state bucket
gcloud projects create urban-pipeline-kd-2026
gcloud storage buckets create gs://urban-pipeline-kd-2026-tf-state --location=US

# 2. Provision infrastructure
cd terraform
cp terraform.tfvars.example terraform.tfvars  # edit your project ID
terraform init && terraform apply

# 3. Python environment
cd ..
python -m venv venv && source venv/Scripts/activate
pip install -r requirements.txt

# 4. Run pipeline manually for a single day
python ingestion/batch/taxi_ingestion.py    --ingestion-date 2022-01-31
python ingestion/batch/weather_ingestion.py --ingestion-date 2022-01-31
python ingestion/batch/load_to_bigquery.py  --ingestion-date 2022-01-31

gcloud storage cp transform/spark_transform.py gs://urban-pipeline-kd-2026-scripts/transform/
gcloud dataproc batches submit pyspark \
    gs://urban-pipeline-kd-2026-scripts/transform/spark_transform.py \
    --batch=urban-pipeline-$(date +%s) --region=us-central1 \
    --service-account=urban-pipeline-sa@urban-pipeline-kd-2026.iam.gserviceaccount.com \
    --version=2.2 --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar \
    -- --project=urban-pipeline-kd-2026 --raw-dataset=raw \
       --staging-dataset=staging --gcs-temp-bucket=urban-pipeline-kd-2026-staging \
       --ingestion-date=2022-01-31

cd dbt/urban_pipeline_dbt && dbt build --profiles-dir ~/.dbt

# 5. OR run end-to-end via Airflow
cd ../../airflow
echo "AIRFLOW_UID=50000" > .env
docker compose up airflow-init
docker compose up -d
# Open http://localhost:8080, login admin/admin, trigger urban_pipeline DAG
~~~

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

## Architecture Decision Record

See [`docs/ADR-001-platform-choices.md`](docs/ADR-001-platform-choices.md) for the rationale on Dataproc Serverless vs cluster, OAuth/ADC vs service account keys, and Docker Airflow vs Cloud Composer.

## Author

**Kaushik Das**
Data Engineer · GCP · BigQuery · dbt · Airflow

[GitHub](https://github.com/kaushikdascult)

## License

MIT