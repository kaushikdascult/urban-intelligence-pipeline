"""
Urban Pipeline — PySpark transformation.

Reads:  raw.taxi_trips, raw.weather_hourly  (BigQuery)
Writes: staging.taxi_trips_enriched         (BigQuery, partitioned by pickup_date,
                                              clustered by pickup_hour, pickup_location_id)

Run via Dataproc Serverless. CLI args make the same code reusable for backfills.
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession, DataFrame, functions as F


# -----------------------------------------------------------------------------
# Logging setup — Cloud Logging picks this up automatically on Dataproc
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("urban-pipeline-transform")


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
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Read raw tables
# -----------------------------------------------------------------------------
def read_taxi(spark: SparkSession, project: str, dataset: str) -> DataFrame:
    log.info(f"Reading {project}.{dataset}.taxi_trips")
    df = (
        spark.read.format("bigquery")
        .option("table", f"{project}.{dataset}.taxi_trips")
        .load()
    )
    log.info(f"Taxi rows: {df.count():,}")
    return df


def read_weather(spark: SparkSession, project: str, dataset: str) -> DataFrame:
    log.info(f"Reading {project}.{dataset}.weather_hourly")
    df = (
        spark.read.format("bigquery")
        .option("table", f"{project}.{dataset}.weather_hourly")
        .load()
    )
    log.info(f"Weather rows: {df.count():,}")
    return df


# -----------------------------------------------------------------------------
# Transform
# -----------------------------------------------------------------------------
def join_and_enrich(taxi: DataFrame, weather: DataFrame) -> DataFrame:
    """Join taxi to weather on pickup hour, derive feature columns."""
    log.info("Aligning timestamps for join")

    # Truncate taxi pickup_datetime down to the hour boundary
    taxi_h = taxi.withColumn(
        "pickup_hour_ts",
        F.date_trunc("hour", F.col("pickup_datetime")),
    )

    # Weather 'time' arrives as STRING from NDJSON load. Cast to timestamp.
    weather_h = weather.withColumn(
        "weather_hour_ts",
        F.to_timestamp(F.col("time")),
    ).select(
        "weather_hour_ts",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "rain",
        "snowfall",
        "wind_speed_10m",
        "wind_direction_10m",
        "weather_code",
        "cloud_cover",
    )

    log.info("Joining taxi -> weather (left join on pickup hour)")
    joined = taxi_h.join(
        weather_h,
        taxi_h["pickup_hour_ts"] == weather_h["weather_hour_ts"],
        how="left",
    ).drop("weather_hour_ts")

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

    return enriched


# -----------------------------------------------------------------------------
# Validate
# -----------------------------------------------------------------------------
def validate(df: DataFrame) -> None:
    """Lightweight data quality checks."""
    total = df.count()
    log.info(f"Enriched row count: {total:,}")

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

    log.info("Sample row distribution by weather_severity:")
    df.groupBy("weather_severity").count().orderBy(F.desc("count")).show(truncate=False)


# -----------------------------------------------------------------------------
# Write
# -----------------------------------------------------------------------------
def write_staging(df: DataFrame, project: str, dataset: str, temp_bucket: str) -> None:
    table = f"{project}.{dataset}.taxi_trips_enriched"
    log.info(
        f"Writing to {table} (partition=pickup_date, cluster=pickup_hour,pickup_location_id)"
    )

    (
        df.write.format("bigquery")
        .option("table", table)
        .option("partitionField", "pickup_date")
        .option("partitionType", "DAY")
        .option("clusteredFields", "pickup_hour,pickup_location_id")
        .option(
            "writeMethod", "indirect"
        )  # ← changed: indirect respects partition config reliably
        .option(
            "createDisposition", "CREATE_IF_NEEDED"
        )  # ← added: explicit table creation flag
        .option("temporaryGcsBucket", temp_bucket)
        .mode("overwrite")
        .save()
    )

    log.info(f"Write complete: {table}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    log.info("=" * 60)
    log.info("Urban Pipeline transform starting")
    log.info(f"Project:         {args.project}")
    log.info(f"Raw dataset:     {args.raw_dataset}")
    log.info(f"Staging dataset: {args.staging_dataset}")
    log.info(f"Temp bucket:     {args.gcs_temp_bucket}")
    log.info("=" * 60)

    spark = (
        SparkSession.builder.appName("urban-pipeline-transform")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    log.info(f"Spark version:   {spark.version}")

    try:
        taxi = read_taxi(spark, args.project, args.raw_dataset)
        weather = read_weather(spark, args.project, args.raw_dataset)

        enriched = join_and_enrich(taxi, weather)
        validate(enriched)
        write_staging(
            enriched, args.project, args.staging_dataset, args.gcs_temp_bucket
        )

        log.info("Done.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
