{{
  config(
    materialized='table',
    description='Daily validations aggregated at line level for METRO and RER — trusted stop-to-line mappings only (high confidence).'
  )
}}

-- Grain: 1 line x 1 day
-- Scope v1: METRO + RER, trusted mappings only (is_trusted_mapping = TRUE in int_validation_stop_line).
-- Aggregates across ticket_type: use fct_validations_line_daily for ticket-level breakdown.

WITH line_daily AS (
  SELECT
    validation_date,
    validation_month,
    line_id,
    line_name,
    transport_mode,
    operator,
    SUM(trusted_validation_count) AS validation_count,
    MAX(trusted_stop_count)       AS mapped_stop_count,
    MAX(ingestion_ts)             AS ingestion_ts
  FROM {{ ref('fct_validations_line_daily') }}
  WHERE transport_mode IN ('METRO', 'RER')
  GROUP BY
    validation_date,
    validation_month,
    line_id,
    line_name,
    transport_mode,
    operator
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['validation_date', 'line_id']) }} AS mart_line_daily_key,

  validation_date,
  validation_month,
  line_id,
  line_name,
  transport_mode,
  operator,
  validation_count,
  mapped_stop_count,
  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM line_daily
