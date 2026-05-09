-- Test: fare_amount must always be positive in fct_trips.
-- Returns rows where fare_amount <= 0 → those rows are violations.
-- Test passes when this query returns zero rows.

select
    trip_id,
    pickup_datetime,
    fare_amount
from {{ ref('fct_trips') }}
where fare_amount <= 0