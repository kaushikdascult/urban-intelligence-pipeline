"""Urban Pipeline orchestration: ingestion → Spark transform → dbt models."""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# Allow imports from /opt/airflow/ingestion
sys.path.insert(0, '/opt/airflow')

PROJECT_ID = 'urban-pipeline-kd-2026'
REGION = 'us-central1'
SA_EMAIL = f'urban-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com'
SCRIPTS_BUCKET = f'{PROJECT_ID}-scripts'
STAGING_BUCKET = f'{PROJECT_ID}-staging'

default_args = {
    'owner': 'urban-pipeline',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_taxi_ingestion(**context):
    from ingestion.batch.taxi_ingestion import main
    main()


def run_weather_ingestion(**context):
    from ingestion.batch.weather_ingestion import main
    main()


def run_load_to_bq(**context):
    from ingestion.batch.load_to_bigquery import main
    main()


with DAG(
    'urban_pipeline',
    default_args=default_args,
    description='End-to-end urban pipeline: ingest → transform → dbt',
    schedule='@daily',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['urban-pipeline'],
) as dag:

    ingest_taxi = PythonOperator(
        task_id='ingest_taxi',
        python_callable=run_taxi_ingestion,
    )

    ingest_weather = PythonOperator(
        task_id='ingest_weather',
        python_callable=run_weather_ingestion,
    )

    load_to_bq = PythonOperator(
        task_id='load_to_bq',
        python_callable=run_load_to_bq,
    )

    spark_transform = DataprocCreateBatchOperator(
        task_id='spark_transform',
        project_id=PROJECT_ID,
        region=REGION,
        batch_id='urban-pipeline-{{ ds_nodash }}-{{ ts_nodash }}',
        batch={
            'pyspark_batch': {
                'main_python_file_uri': f'gs://{SCRIPTS_BUCKET}/transform/spark_transform.py',
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar'],
                'args': [
                    f'--project={PROJECT_ID}',
                    '--raw-dataset=raw',
                    '--staging-dataset=staging',
                    f'--gcs-temp-bucket={STAGING_BUCKET}',
                ],
            },
            'environment_config': {
                'execution_config': {
                    'service_account': SA_EMAIL,
                },
            },
            'runtime_config': {
                'version': '2.2',
            },
        },
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt/urban_pipeline_dbt && dbt run --profiles-dir .',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt/urban_pipeline_dbt && dbt test --profiles-dir .',
    )

    [ingest_taxi, ingest_weather] >> load_to_bq >> spark_transform >> dbt_run >> dbt_test