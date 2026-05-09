-- Analysis: Data freshness check
-- Run in BQ to verify the pipeline ran recently.

select
    'fct_trips' as table_name,
    count(*) as total_rows,
    min(pickup_datetime) as earliest_pickup,
    max(pickup_datetime) as latest_pickup,
    count(distinct pickup_date) as distinct_partitions,
    timestamp_diff(current_timestamp(), max(pickup_datetime), hour) as hours_since_latest
from {{ ref('fct_trips') }}