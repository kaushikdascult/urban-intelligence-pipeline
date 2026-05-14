"""
Dataproc Serverless PySpark transformation:
raw.taxi_trips + raw.weather_hourly  →  staging.taxi_trips_enriched

Reads one day's partition from each raw table, joins on the truncated hour,
derives weather feature columns, and writes the result into a single
partition of the Terraform-managed staging table.

Write pattern: DELETE-then-APPEND via BQ Python client + Spark INDIRECT mode.
The BQ Spark connector's DIRECT write mode is destructive to externally-managed
schemas (silently strips Terraform's NUMERIC precision, partition spec, and
column nullability). INDIRECT mode preserves the target table's schema but
only supports full-table OVERWRITE, not partition-scoped writes. The
workaround: DELETE the partition's existing rows via google-cloud-bigquery
(available in the Dataproc Serverless conda env), then APPEND via Spark.

Usage:
    spark_transform.py --project urban-pipeline-kd-2026 \\
                       --raw-dataset raw \\
                       --staging-dataset staging \\
                       --gcs-temp-bucket urban-pipeline-kd-2026-staging \\
                       --ingestion-date 2022-01-15
"""

import argparse
import logging

from google.cloud import bigquery
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# CLI args
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform raw taxi+weather into enriched staging table."
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--raw-dataset", default="raw", help="Source BQ dataset")
    parser.add_argument(
        "--staging-dataset", default="staging", help="Target BQ dataset"
    )
    parser.add_argument(
        "--gcs-temp-bucket",
        required=True,
        help="GCS bucket name for Spark BQ connector temp files (no gs:// prefix)",
    )
    parser.add_argument(
        "--ingestion-date",
        required=True,
        help="ISO date (YYYY-MM-DD) to process. Pushed down as a predicate "
        "on pickup_date / weather_date in both raw tables, and used to "
        "scope the DELETE on staging before the Spark append.",
    )
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Read raw tables, pushed-down to one day
# -----------------------------------------------------------------------------
def read_taxi(
    spark: SparkSession, project: str, dataset: str, ingestion_date: str
) -> DataFrame:
    table = f"{project}.{dataset}.taxi_trips"
    log.info(f"Reading {table} for pickup_date = {ingestion_date}")
    df = (
        spark.read.format("bigquery")
        .option("table", table)
        .option("filter", f"pickup_date = '{ingestion_date}'")
        .load()
    )
    log.info(f"Taxi rows: {df.count():,}")
    return df


def read_weather(
    spark: SparkSession, project: str, dataset: str, ingestion_date: str
) -> DataFrame:
    table = f"{project}.{dataset}.weather_hourly"
    log.info(f"Reading {table} for weather_date = {ingestion_date}")
    df = (
        spark.read.format("bigquery")
        .option("table", table)
        .option("filter", f"weather_date = '{ingestion_date}'")
        .load()
    )
    log.info(f"Weather rows: {df.count():,}")
    return df


# -----------------------------------------------------------------------------
# Join + enrich + normalize types to match Terraform schema
# -----------------------------------------------------------------------------
def join_and_enrich(taxi: DataFrame, weather: DataFrame) -> DataFrame:
    log.info("Joining taxi and weather on truncated pickup hour")

    taxi = taxi.withColumn(
        "pickup_hour_ts",
        F.date_trunc("hour", F.col("pickup_datetime")),
    )
    weather = weather.withColumnRenamed("time", "weather_hour_ts")

    joined = taxi.join(
        weather,
        taxi["pickup_hour_ts"] == weather["weather_hour_ts"],
        how="left",
    ).drop(
        "weather_hour_ts",
        "pickup_hour_ts",
        "weather_date",
        "city",
        "latitude",
        "longitude",
        "ingested_at_utc",
        "surface_pressure",
    )

    log.info("Deriving weather feature columns")
    enriched = (
        joined.withColumn("is_raining", F.col("rain") > 0)
        .withColumn("is_snowing", F.col("snowfall") > 0)
        .withColumn(
            "weather_severity",
            F.when(F.col("snowfall") > 1.0, "blizzard")
            .when(F.col("rain") > 5.0, "heavy_rain")
            .when(F.col("rain") > 0, "light_rain")
            .when(F.col("temperature_2m") < -5.0, "freezing")
            .otherwise("clear"),
        )
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2),
            ).otherwise(F.lit(None)),
        )
    )

    log.info("Normalizing column types to match Terraform-managed schema")
    enriched = (
        enriched.withColumn("vendor_id", F.col("vendor_id").cast(IntegerType()))
        .withColumn(
            "pickup_location_id", F.col("pickup_location_id").cast(IntegerType())
        )
        .withColumn(
            "dropoff_location_id", F.col("dropoff_location_id").cast(IntegerType())
        )
        # Money columns: cast to Decimal(10,2) to match the Terraform-managed
        # NUMERIC(10,2) target columns exactly. INDIRECT mode does NOT coerce
        # DOUBLE -> NUMERIC; the BQ load job rejects the schema mismatch
        # before loading. Decimal(10,2) -> NUMERIC(10,2) maps cleanly.
        .withColumn("fare_amount", F.col("fare_amount").cast(DecimalType(10, 2)))
        .withColumn("extra", F.col("extra").cast(DecimalType(10, 2)))
        .withColumn("mta_tax", F.col("mta_tax").cast(DecimalType(10, 2)))
        .withColumn("tip_amount", F.col("tip_amount").cast(DecimalType(10, 2)))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DecimalType(10, 2)))
        .withColumn("imp_surcharge", F.col("imp_surcharge").cast(DecimalType(10, 2)))
        .withColumn("airport_fee", F.col("airport_fee").cast(DecimalType(10, 2)))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(10, 2)))
        .withColumn("tip_pct", F.col("tip_pct").cast(DoubleType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("pickup_date", F.col("pickup_date").cast(DateType()))
        .withColumn("is_raining", F.col("is_raining").cast(BooleanType()))
        .withColumn("is_snowing", F.col("is_snowing").cast(BooleanType()))
        .withColumn("weather_severity", F.col("weather_severity").cast(StringType()))
        # Weather integer columns: raw stores these as FLOAT64, but the
        # staging table's Terraform schema declares them INT64. Cast to
        # match the write target.
        .withColumn(
            "relative_humidity_2m",
            F.col("relative_humidity_2m").cast(IntegerType()),
        )
        .withColumn(
            "wind_direction_10m",
            F.col("wind_direction_10m").cast(IntegerType()),
        )
        .withColumn("cloud_cover", F.col("cloud_cover").cast(IntegerType()))
    )

    return enriched


# -----------------------------------------------------------------------------
# Validate
# -----------------------------------------------------------------------------
def validate(df: DataFrame) -> None:
    log.info("Validation")
    total = df.count()
    log.info(f"Total rows: {total:,}")

    if total == 0:
        log.error("Empty result set — aborting before write")
        raise ValueError("Transformation produced 0 rows")

    weather_join_rate = df.filter(F.col("temperature_2m").isNotNull()).count() / total
    log.info(f"Weather join rate: {weather_join_rate:.1%}")

    if weather_join_rate < 0.5:
        log.warning(
            f"Low weather join rate ({weather_join_rate:.1%}). "
            "Most taxi rows did not match a weather hour. "
            "Check timezone alignment."
        )

    log.info("Row distribution by weather_severity:")
    df.groupBy("weather_severity").count().orderBy(F.desc("count")).show(truncate=False)


# -----------------------------------------------------------------------------
# DELETE-then-APPEND
# -----------------------------------------------------------------------------
def delete_partition(project: str, dataset: str, ingestion_date: str) -> int:
    """DELETE the day's rows from staging via the BQ Python client.

    Required because Spark's INDIRECT-mode 'overwrite' replaces the whole
    table (not just a partition). We pre-clear the target partition here,
    then Spark appends. Same pattern as load_to_bigquery.py uses for raw.
    """
    table = f"{project}.{dataset}.taxi_trips_enriched"
    client = bigquery.Client(project=project)

    sql = f"DELETE FROM `{table}` WHERE pickup_date = @ingestion_date"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ingestion_date", "DATE", ingestion_date),
        ]
    )
    log.info(f"DELETE FROM {table} WHERE pickup_date = '{ingestion_date}'")
    job = client.query(sql, job_config=job_config)
    job.result()
    deleted = job.num_dml_affected_rows or 0
    log.info(f"Deleted {deleted:,} pre-existing rows for {ingestion_date}")
    return deleted


def write_staging(
    df: DataFrame,
    project: str,
    dataset: str,
    temp_bucket: str,
    ingestion_date: str,
) -> None:
    table = f"{project}.{dataset}.taxi_trips_enriched"

    # Pre-clear this partition so the Spark APPEND is idempotent.
    delete_partition(project, dataset, ingestion_date)

    log.info(f"Appending to {table} (INDIRECT write mode)")
    log.info(
        "createDisposition=CREATE_NEVER ensures the Terraform-managed "
        "schema is respected; the write fails loudly if the table is missing."
    )

    (
        df.write.format("bigquery")
        .option("table", table)
        .option("writeMethod", "indirect")
        .option("partitionField", "pickup_date")
        .option("partitionType", "DAY")
        .option("clusteredFields", "pickup_hour,pickup_location_id")
        .option("createDisposition", "CREATE_NEVER")
        .option("temporaryGcsBucket", temp_bucket)
        .mode("append")
        .save()
    )

    log.info(f"Wrote {df.count():,} rows for partition {ingestion_date}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    args = parse_args()

    log.info("=" * 60)
    log.info("Spark transform starting")
    log.info(f"Project:         {args.project}")
    log.info(f"Raw dataset:     {args.raw_dataset}")
    log.info(f"Staging dataset: {args.staging_dataset}")
    log.info(f"Temp bucket:     {args.gcs_temp_bucket}")
    log.info(f"Ingestion date:  {args.ingestion_date}")
    log.info("=" * 60)

    spark = (
        SparkSession.builder.appName("urban-pipeline-transform")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    log.info(f"Spark version:   {spark.version}")

    try:
        taxi = read_taxi(spark, args.project, args.raw_dataset, args.ingestion_date)
        weather = read_weather(
            spark, args.project, args.raw_dataset, args.ingestion_date
        )

        enriched = join_and_enrich(taxi, weather)
        validate(enriched)
        write_staging(
            enriched,
            args.project,
            args.staging_dataset,
            args.gcs_temp_bucket,
            args.ingestion_date,
        )

        log.info("Done.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
