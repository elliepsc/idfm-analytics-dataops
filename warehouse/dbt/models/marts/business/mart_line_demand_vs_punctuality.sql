{{
  config(
    materialized='table',
    description='Monthly demand vs punctuality cross by line. Grain: 1 line x 1 month. Rewritten on mart_validations_line_daily (METRO+RER trusted) + fct_punctuality_monthly (RER+TRAIN). Replaces legacy line_code_mapping seed dependency.'
  )
}}

-- V4 rewrite: demand now comes from mart_validations_line_daily (trusted stop→line bridge)
-- instead of line_code_mapping.csv (SNCF-only seed, ~10 lines, no metro).
-- Metro lines appear with no_punctuality_data profile — this is intentional and honest.
--
-- Business questions answered:
--   1. Do high-demand lines degrade faster than low-demand ones?
--   2. Which lines remain resilient under ridership pressure?
--   3. Are some lines structurally fragile even without high demand?

WITH demand_monthly AS (
  SELECT
    validation_month,
    line_id,
    line_name,
    transport_mode,
    operator,
    SUM(validation_count) AS line_validations
  FROM {{ ref('mart_validations_line_daily') }}
  GROUP BY validation_month, line_id, line_name, transport_mode, operator
),

network_monthly AS (
  SELECT
    validation_month,
    SUM(line_validations) AS network_total
  FROM demand_monthly
  GROUP BY 1
),

demand_indexed AS (
  SELECT
    v.validation_month,
    v.line_id,
    v.line_name,
    v.transport_mode,
    v.operator,
    v.line_validations,
    n.network_total,
    ROUND(
      {{ safe_divide('v.line_validations', 'n.network_total') }} * 100,
      2
    ) AS demand_index_pct,

    -- Demand tier relative to METRO+RER network this month
    NTILE(3) OVER (
      PARTITION BY v.validation_month
      ORDER BY v.line_validations
    ) AS demand_tercile  -- 1=low, 2=medium, 3=high

  FROM demand_monthly v
  INNER JOIN network_monthly n ON v.validation_month = n.validation_month
),

combined AS (
  SELECT
    d.validation_month,
    d.line_id,
    d.line_name,
    d.transport_mode,
    d.operator,
    d.line_validations,
    d.demand_index_pct,
    d.demand_tercile,
    p.punctuality_rate,
    p.quality_category,
    t.target_punctuality_rate,
    ROUND(p.punctuality_rate - t.target_punctuality_rate, 2) AS gap_to_target

  FROM demand_indexed d
  LEFT JOIN {{ ref('fct_punctuality_monthly') }} p
    ON d.line_id = p.line_id AND d.validation_month = p.month_date
  LEFT JOIN {{ ref('line_punctuality_target') }} t
    ON UPPER(TRIM(d.line_id)) = UPPER(TRIM(t.line_id))
)

SELECT
  validation_month,
  line_id,
  line_name,
  transport_mode,
  operator,
  line_validations,
  demand_index_pct,
  demand_tercile,
  punctuality_rate,
  quality_category,
  target_punctuality_rate,
  gap_to_target,

  CASE
    WHEN punctuality_rate IS NULL
      THEN 'no_punctuality_data'

    WHEN demand_tercile = 3 AND punctuality_rate >= 90.0
      THEN 'resilient_under_pressure'

    WHEN demand_tercile = 3 AND punctuality_rate < 90.0
      THEN 'fragile_under_pressure'

    WHEN demand_tercile = 1 AND punctuality_rate < 88.0
      THEN 'structurally_fragile'

    WHEN demand_tercile = 1 AND punctuality_rate >= 90.0
      THEN 'stable_low_demand'

    ELSE 'moderate'
  END AS demand_punctuality_profile,

  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM combined
ORDER BY validation_month, demand_index_pct DESC
