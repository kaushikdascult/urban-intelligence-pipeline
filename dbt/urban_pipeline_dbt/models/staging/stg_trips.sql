{{ config(materialized='view') }}

SELECT
    -- Surrogate key: hash enough fields that no two trips collide
    TO_HEX(MD5(CONCAT(
        CAST(pickup_datetime AS STRING),
        CAST(dropoff_datetime AS STRING),
        COALESCE(pickup_location_id, ''),
        COALESCE(dropoff_location_id, ''),
        CAST(fare_amount AS STRING),
        CAST(tip_amount AS STRING),
        CAST(trip_distance AS STRING)
    ))) AS trip_id,
    
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    day_of_week,
    
    CAST(passenger_count AS INT64) AS passenger_count,
    CAST(trip_distance AS FLOAT64) AS trip_distance,
    trip_duration_minutes,
    
    CAST(fare_amount AS FLOAT64) AS fare_amount,
    CAST(tip_amount AS FLOAT64) AS tip_amount,
    CAST(total_amount AS FLOAT64) AS total_amount,
    CAST(tip_pct AS FLOAT64) AS tip_pct,
    
    pickup_location_id,
    dropoff_location_id,
    
    payment_type,
    
    -- Weather features
    temperature_2m AS temperature_c,
    relative_humidity_2m AS humidity_pct,
    rain AS rain_mm,
    snowfall AS snowfall_cm,
    wind_speed_10m AS wind_speed_kmh,
    is_raining,
    is_snowing,
    weather_severity

FROM {{ source('staging', 'taxi_trips_enriched') }}
WHERE fare_amount > 0
  AND trip_duration_minutes BETWEEN 0 AND 1440