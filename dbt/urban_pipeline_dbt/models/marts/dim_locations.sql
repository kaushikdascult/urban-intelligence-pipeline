{{ config(materialized='table') }}

WITH all_locations AS (
    SELECT DISTINCT pickup_location_id AS location_id FROM {{ ref('stg_trips') }} WHERE pickup_location_id IS NOT NULL
    UNION DISTINCT
    SELECT DISTINCT dropoff_location_id AS location_id FROM {{ ref('stg_trips') }} WHERE dropoff_location_id IS NOT NULL
),
trip_volumes AS (
    SELECT
        pickup_location_id AS location_id,
        COUNT(*) AS pickup_count
    FROM {{ ref('stg_trips') }}
    WHERE pickup_location_id IS NOT NULL
    GROUP BY 1
)

SELECT
    l.location_id,
    COALESCE(v.pickup_count, 0) AS total_pickups,
    CASE
        WHEN COALESCE(v.pickup_count, 0) > 10000 THEN 'high_volume'
        WHEN COALESCE(v.pickup_count, 0) > 1000 THEN 'medium_volume'
        ELSE 'low_volume'
    END AS volume_tier
FROM all_locations l
LEFT JOIN trip_volumes v ON l.location_id = v.location_id