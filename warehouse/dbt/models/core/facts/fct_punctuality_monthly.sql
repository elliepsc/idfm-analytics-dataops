-- Punctuality fact table at the grain of 1 line × 1 month
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
    description='Punctuality fact table (grain = 1 line × 1 month)'
  )
}}

WITH punctuality AS (
  SELECT * FROM {{ ref('stg_punctuality_monthly') }}

  {% if is_incremental() %}
  WHERE month_date > (SELECT MAX(month_date) FROM {{ this }})
  {% endif %}
)

SELECT
  -- Surrogate key
  {{ dbt_utils.generate_surrogate_key(['line_id', 'month_date']) }} AS punctuality_key,

  -- Date
  month_date,
  year,
  -- FIX V2: renamed 'month' → 'month_num' in stg_punctuality_monthly
  -- to avoid conflict with the reserved SQL keyword 'month'.
  month_num,

  -- Dimension FK
  line_id,

  -- Metrics
  -- FIX V2: removed trains_planned and trains_departed — these fields do NOT exist
  -- in the ponctualite-mensuelle-transilien dataset (confirmed 2026-02-27).
  punctuality_rate,
  quality_category,

  -- Metadata
  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM punctuality
