{% snapshot dim_locations_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='location_id',
      strategy='check',
      check_cols=['total_pickups', 'volume_tier'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    location_id,
    total_pickups,
    volume_tier
FROM {{ ref('dim_locations') }}

{% endsnapshot %}
