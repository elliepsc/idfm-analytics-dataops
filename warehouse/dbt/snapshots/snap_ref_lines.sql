{% snapshot snap_ref_lines %}

{{
    config(
        target_schema='transport_snapshots',
        unique_key='line_id',
        strategy='check',
        check_cols=['line_name', 'transport_mode', 'operator'],
    )
}}

SELECT
    line_id,
    line_name,
    transport_mode,
    operator,
    is_active
FROM {{ ref('dim_line') }}

{% endsnapshot %}
