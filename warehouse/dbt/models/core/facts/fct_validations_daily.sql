-- Weekly validations fact table at the grain of 1 stop × 1 day × 1 ticket type


{{
  config(
    materialized='incremental',
    unique_key='validation_key',
    partition_by={
      "field": "validation_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['stop_id', 'line_id', 'ticket_type'],
    description='Fait des validations (grain = 1 arrêt × 1 jour × 1 type titre)'
  )
}}

WITH validations AS (
  SELECT * FROM {{ ref('stg_validations_rail_daily') }}

  {% if is_incremental() %}
  -- Mode incrémental : seulement les nouvelles dates
  WHERE validation_date > (SELECT MAX(validation_date) FROM {{ this }})
  {% endif %}
)

SELECT
  -- Clé composite
  {{ dbt_utils.generate_surrogate_key(['stop_id', 'validation_date', 'ticket_type']) }} AS validation_key,

  -- Dates (pour partitionnement)
  validation_date,
  validation_month,
  year,
  month,
  day_of_week,

  -- Dimensions (FK)
  stop_id,
  line_id,
  ticket_type,

  -- Métriques
  validation_count,

  -- Metadata
  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM validations
