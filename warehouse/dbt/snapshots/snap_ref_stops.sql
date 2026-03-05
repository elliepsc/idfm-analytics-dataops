{% snapshot snap_ref_stops %}

{{
    config(
        target_schema='transport_snapshots',
        unique_key='stop_id',
        strategy='check',
        check_cols=['stop_name', 'city', 'stop_type'],
    )
}}

SELECT
    stop_id,
    stop_name,
    city,
    stop_type,
    is_active
FROM {{ ref('dim_stop') }}

{% endsnapshot %}
