{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT date AS date_day
    FROM UNNEST(GENERATE_DATE_ARRAY('2022-01-01', '2022-12-31', INTERVAL 1 DAY)) AS date
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    FORMAT_DATE('%B', date_day) AS month_name,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    FORMAT_DATE('%A', date_day) AS day_name,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine