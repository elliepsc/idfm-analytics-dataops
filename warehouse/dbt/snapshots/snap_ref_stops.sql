{% snapshot snap_ref_stops %}
{{
    config(
        target_schema='transport_snapshots',
        unique_key='stop_id',
        strategy='check',
        check_cols=['stop_name', 'latitude', 'longitude'],
    )
}}
SELECT
    stop_id,
    stop_name,
    latitude,
    longitude
FROM {{ ref('dim_stop') }}
{% endsnapshot %}
