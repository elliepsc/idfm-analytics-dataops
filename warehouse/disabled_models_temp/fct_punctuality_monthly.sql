-- Ponctuality fact table at the grain of 1 line × 1 month

{{
  config(
    materialized='incremental',
    unique_key='punctuality_key',
    partition_by={
      "field": "month_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['line_id'],
    description='Fait de la ponctualité (grain = 1 ligne × 1 mois)'
  )
}}

WITH punctuality AS (
  SELECT * FROM {{ ref('stg_punctuality_monthly') }}

  {% if is_incremental() %}
  WHERE month_date > (SELECT MAX(month_date) FROM {{ this }})
  {% endif %}
)

SELECT
  -- Clé composite
  {{ dbt_utils.generate_surrogate_key(['line_id', 'month_date']) }} AS punctuality_key,

  -- Date
  month_date,
  year,
  month,

  -- Dimension (FK)
  line_id,

  -- Métriques
  punctuality_rate,
  trains_planned,
  trains_departed,
  quality_category,

  -- Metadata
  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM punctuality
