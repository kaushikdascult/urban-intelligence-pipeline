-- Test: pickup_datetime cannot be in the future.
-- Catches data quality issues (clock drift, bad source data).

select
    trip_id,
    pickup_datetime
from {{ ref('fct_trips') }}
where pickup_datetime > current_timestamp()
