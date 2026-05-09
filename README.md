# Urban Intelligence Pipeline

End-to-end batch data pipeline on Google Cloud Platform: NYC taxi trip data joined with hourly weather data, processed through bronze → silver → gold layers, orchestrated by Airflow.

## Architecture

~~~
NYC Taxi (BQ public)        Open-Meteo API
       │                          │
       └────── Python ingestion ──┘
                      │
                      ▼
       GCS raw bucket (parquet + JSON)
       partitioned by ingestion_date
                      │
                      ▼
       BigQuery raw dataset
       (raw.taxi_trips, raw.weather_hourly)
                      │
                      ▼
       Dataproc Serverless (PySpark)
       - Joins taxi + weather on pickup hour
       - Derives weather_severity, tip_pct
       - Partitioned + clustered output
                      │
                      ▼
       BigQuery staging.taxi_trips_enriched
                      │
                      ▼
       dbt models (BigQuery)
       - stg_trips (view)
       - dim_dates, dim_locations
       - fct_trips (partitioned + clustered fact)
                      │
                      ▼
       Analysis-ready marts
~~~

Orchestrated end-to-end by an **Airflow DAG** (Docker locally; Composer-ready for cloud).

## Stack

- **Cloud**: Google Cloud Platform (free trial, ~$1.50 spent)
- **IaC**: Terraform 1.5+ (24 resources, GCS-backed remote state)
- **Storage**: Google Cloud Storage, BigQuery
- **Compute**: Dataproc Serverless (PySpark 3.5)
- **Transformation**: dbt-bigquery 1.8 with OAuth via ADC
- **Orchestration**: Apache Airflow 2.10 (Docker; LocalExecutor)
- **Languages**: Python 3.11, SQL, HCL, YAML

## Project structure

~~~
urban-pipeline/
├── terraform/                    # IaC: 24 resources (APIs, GCS, BQ, IAM)
├── ingestion/batch/              # Python: extract → GCS → BQ raw
│   ├── taxi_ingestion.py         # 500K rows from NYC TLC public dataset
│   ├── weather_ingestion.py      # 744 hours from Open-Meteo
│   └── load_to_bigquery.py
├── transform/
│   └── spark_transform.py        # Dataproc Serverless PySpark job
├── dbt/urban_pipeline_dbt/
│   ├── models/staging/           # Surrogate keys, type safety
│   ├── models/marts/             # Star schema (dim + fact)
│   ├── tests/                    # Custom singular tests
│   └── analyses/                 # Monitoring SQL
├── airflow/
│   ├── docker-compose.yml        # Postgres + scheduler + webserver
│   └── dags/urban_pipeline_dag.py
├── tests/                        # pytest smoke tests (11 tests)
└── README.md
~~~

## Data layers

| Layer  | Location                                                         | Format                                                                                | Volume                          |
|--------|------------------------------------------------------------------|---------------------------------------------------------------------------------------|---------------------------------|
| Raw    | `gs://urban-pipeline-kd-2026-raw/taxi/ingestion_date=YYYY-MM-DD/` | Parquet + JSON                                                                        | 485K trips + 744 weather hours  |
| Bronze | BigQuery `raw.*`                                                 | Native BQ                                                                             | Same as above                   |
| Silver | BigQuery `staging.taxi_trips_enriched`                           | Partitioned by `pickup_date`, clustered by `pickup_hour, pickup_location_id`          | 484K rows, 31 partitions        |
| Gold   | BigQuery `marts.{dim_dates, dim_locations, fct_trips}`           | Star schema                                                                           | fct_trips: 484K rows, 18.5 MB physical |

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

# 4. Run pipeline manually (one-shot)
python ingestion/batch/taxi_ingestion.py
python ingestion/batch/weather_ingestion.py
python ingestion/batch/load_to_bigquery.py

gcloud storage cp transform/spark_transform.py gs://urban-pipeline-kd-2026-scripts/transform/
gcloud dataproc batches submit pyspark \
    gs://urban-pipeline-kd-2026-scripts/transform/spark_transform.py \
    --batch=urban-pipeline-$(date +%s) --region=us-central1 \
    --service-account=urban-pipeline-sa@urban-pipeline-kd-2026.iam.gserviceaccount.com \
    --version=2.2 --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar \
    -- --project=urban-pipeline-kd-2026 --raw-dataset=raw \
       --staging-dataset=staging --gcs-temp-bucket=urban-pipeline-kd-2026-staging

cd dbt/urban_pipeline_dbt && dbt build --profiles-dir ~/.dbt

# 5. OR run the whole thing via Airflow
cd ../../airflow
echo "AIRFLOW_UID=50000" > .env
docker compose up airflow-init
docker compose up -d
# Open http://localhost:8080, login admin/admin, trigger urban_pipeline DAG
~~~

## Tests

~~~bash
# Python smoke tests
pytest tests/ -v

# dbt tests (11 generic + 3 custom singular)
cd dbt/urban_pipeline_dbt && dbt test --profiles-dir ~/.dbt
~~~

## Cost

Approximate cost for one full pipeline run:

| Component                                  | Cost                              |
|--------------------------------------------|-----------------------------------|
| GCS storage (raw bucket, ~50 MB)           | <$0.01                            |
| BigQuery storage (~120 MB physical)        | <$0.01                            |
| BQ public-dataset query (500K rows scanned)| $0 (covered by user's free tier)  |
| Dataproc Serverless batch (2 min)          | ~$0.10                            |
| dbt run on BQ (3 tables, 90 MB processed)  | ~$0.01                            |
| **Total per run**                          | **~$0.12**                        |

## Key engineering decisions

- **Dataproc Serverless over cluster**: zero idle cost, scales to zero between runs.
- **Indirect write mode + `createDisposition=CREATE_IF_NEEDED`**: BQ Spark connector silently dropped partitioning config in direct mode.
- **OAuth/ADC over service account keys**: org policy `iam.disableServiceAccountKeyCreation` enforced; ADC volume-mounted into Airflow containers.
- **Hive-style date partitioning in raw bucket**: `taxi/ingestion_date=YYYY-MM-DD/` enables efficient time-range scans.
- **Surrogate keys via wide MD5**: `MD5(pickup_dt, dropoff_dt, pickup_loc, dropoff_loc, fare, tip, distance)` — narrower hashes produced ~36 collisions.
- **Cluster on `pickup_hour, pickup_location_id`**: matches the most common analytical query pattern (hourly breakdowns by zone).

## Sample analytical query

~~~sql
-- Top 10 hours by trip volume during the January 2022 NYC blizzard
select
    pickup_date,
    pickup_hour,
    count(*) as trips,
    round(avg(fare_amount), 2) as avg_fare
from `urban-pipeline-kd-2026.marts.fct_trips`
where weather_severity = 'blizzard'
group by 1, 2
order by trips desc
limit 10
~~~

The pipeline correctly captured the January 7, 2022 NYC blizzard: snowfall of 1.05–1.33 cm/hr from 4–7 AM, with average fare jumping to $34.64 at 5 AM.

## License

MIT