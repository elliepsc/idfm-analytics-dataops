{{
  config(
    materialized='incremental',
    unique_key='validation_key',
    partition_by={
      "field": "validation_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['stop_id', 'ticket_type'],
    description='Validations fact table (grain = 1 stop × 1 day × 1 ticket type)'
  )
}}

-- Validations fact table at the grain of 1 stop × 1 day × 1 ticket type
-- FIX V2: removed line_id from cluster_by — not available in raw_validations.
-- FIX V2: replaced line_id with line_code_trns and line_code_res.

WITH validations AS (
  SELECT * FROM {{ ref('stg_validations_rail_daily') }}

  {% if is_incremental() %}
  WHERE validation_date > (SELECT MAX(validation_date) FROM {{ this }})
  {% endif %}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stop_id', 'validation_date', 'ticket_type']) }} AS validation_key,

  validation_date,
  validation_month,
  year,
  month,
  day_of_week,

  stop_id,
  line_code_trns,
  line_code_res,
  ticket_type,

  validation_count,

  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM validations


