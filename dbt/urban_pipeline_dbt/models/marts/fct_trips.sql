{{ config(
    materialized='table',
    partition_by={'field': 'pickup_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['pickup_hour', 'pickup_location_id']
) }}

SELECT
    t.trip_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_date,
    t.pickup_hour,
    t.day_of_week,
    
    t.pickup_location_id,
    pl.volume_tier AS pickup_volume_tier,
    t.dropoff_location_id,
    dl.volume_tier AS dropoff_volume_tier,
    
    t.passenger_count,
    t.trip_distance,
    t.trip_duration_minutes,
    
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.tip_pct,
    
    t.weather_severity,
    t.is_raining,
    t.is_snowing,
    t.temperature_c,
    t.rain_mm,
    t.snowfall_cm,
    
    d.day_name,
    d.is_weekend,
    d.month_name

FROM {{ ref('stg_trips') }} t
LEFT JOIN {{ ref('dim_locations') }} pl ON t.pickup_location_id = pl.location_id
LEFT JOIN {{ ref('dim_locations') }} dl ON t.dropoff_location_id = dl.location_id
LEFT JOIN {{ ref('dim_dates') }} d ON t.pickup_date = d.date_day