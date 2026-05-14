# urban_pipeline_dbt

dbt project for the Urban Intelligence Pipeline — the modelling layer that
turns `staging.taxi_trips_enriched` into an analysis-ready star schema in
BigQuery.

## Models

| Model | Materialization | Notes |
| --- | --- | --- |
| `stg_trips` | view | Surrogate key (MD5 hash), type casts, quality filter (`fare_amount > 0`, valid duration) |
| `dim_dates` | table | Date spine 2022-01-01 → 2022-12-31 |
| `dim_locations` | table | Pickup/dropoff zones with volume tiering |
| `fct_trips` | incremental | One row per trip, enriched with weather + dimension keys. `insert_overwrite` strategy, partitioned by `pickup_date` |

## Snapshots

| Snapshot | Strategy | Notes |
| --- | --- | --- |
| `dim_locations_snapshot` | check | SCD2 history of `volume_tier` / `total_pickups` per zone |

## Common commands

~~~bash
dbt run --profiles-dir ~/.dbt                      # build all models
dbt run --select fct_trips --profiles-dir ~/.dbt   # incremental run (latest partition only)
dbt run --select fct_trips --full-refresh --profiles-dir ~/.dbt   # full rebuild
dbt snapshot --profiles-dir ~/.dbt                 # capture dim_locations SCD2
dbt test --profiles-dir ~/.dbt                     # 14 data tests
~~~

See the [repository README](../../README.md) for the full pipeline architecture.