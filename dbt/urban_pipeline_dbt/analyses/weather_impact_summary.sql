-- Analysis: Weather impact on fares and trip counts
-- Demonstrates the value of joining taxi data with weather.

select
    weather_severity,
    count(*) as trip_count,
    round(avg(fare_amount), 2) as avg_fare,
    round(avg(tip_pct), 2) as avg_tip_pct,
    round(avg(trip_distance), 2) as avg_distance_miles
from {{ ref('fct_trips') }}
where weather_severity is not null
group by weather_severity
order by trip_count desc