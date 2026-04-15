{{
  config(
    materialized='table',
    description='Monthly validations aggregated at line level for METRO and RER — trusted mappings only. Grain: 1 line x 1 month.'
  )
}}

-- Source: mart_validations_line_daily (already filtered to METRO + RER, trusted only)
-- Aggregates across ticket_type and day. Use mart_validations_line_daily for daily breakdown.

SELECT
  {{ dbt_utils.generate_surrogate_key(['validation_month', 'line_id']) }} AS mart_line_monthly_key,

  validation_month,
  line_id,
  line_name,
  transport_mode,
  operator,
  SUM(validation_count)           AS validation_count,
  MAX(mapped_stop_count)          AS mapped_stop_count,
  COUNT(DISTINCT validation_date) AS days_with_data,
  MAX(ingestion_ts)               AS ingestion_ts,
  CURRENT_TIMESTAMP()             AS dbt_updated_at

FROM {{ ref('mart_validations_line_daily') }}
GROUP BY
  validation_month,
  line_id,
  line_name,
  transport_mode,
  operator
