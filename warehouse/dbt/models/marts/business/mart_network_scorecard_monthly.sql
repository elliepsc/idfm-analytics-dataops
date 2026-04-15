{{
  config(
    materialized='table',
    description='Network scorecard: demand + quality per line and month. Grain: 1 line x 1 month. Rewritten on mart_validations_line_daily (METRO+RER) + fct_punctuality_monthly (RER+TRAIN). Fixes: line-level demand (was network-level), LAG partitioned by line_id.'
  )
}}

-- V4 rewrite — replaces the old model that joined network-wide validations to each
-- punctuality line (semantically wrong: the same total was repeated on every line).
--
-- New approach:
--   - Demand comes from mart_validations_line_daily (trusted METRO + RER)
--   - FULL OUTER JOIN with punctuality so all lines appear:
--       * METRO: demand only, punctuality = NULL
--       * RER: both demand and punctuality
--       * TRAIN: punctuality only, demand = 0
--   - MoM growth is now PARTITIONED BY line_id (was a global window — bug in V3)

WITH demand_monthly AS (
  SELECT
    validation_month                AS month_date,
    line_id,
    line_name,
    transport_mode,
    operator,
    SUM(validation_count)           AS total_validations,
    COUNT(DISTINCT validation_date) AS days_with_data,
    MAX(mapped_stop_count)          AS stops_count
  FROM {{ ref('mart_validations_line_daily') }}
  GROUP BY validation_month, line_id, line_name, transport_mode, operator
),

punctuality_monthly AS (
  SELECT
    line_id,
    month_date,
    punctuality_rate,
    quality_category
  FROM {{ ref('fct_punctuality_monthly') }}
),

combined AS (
  SELECT
    COALESCE(d.month_date, p.month_date)        AS month_date,
    COALESCE(d.line_id,    p.line_id)           AS line_id,
    d.line_name,
    d.transport_mode,
    d.operator,
    COALESCE(d.total_validations, 0)            AS total_validations,
    COALESCE(d.days_with_data, 0)               AS days_with_data,
    COALESCE(d.stops_count, 0)                  AS stops_count,
    SAFE_DIVIDE(
      COALESCE(d.total_validations, 0),
      NULLIF(COALESCE(d.days_with_data, 0), 0)
    )                                           AS avg_daily_validations,
    p.punctuality_rate,
    p.quality_category
  FROM demand_monthly d
  FULL OUTER JOIN punctuality_monthly p
    ON d.line_id = p.line_id AND d.month_date = p.month_date
  WHERE COALESCE(d.month_date, p.month_date) IS NOT NULL
    AND COALESCE(d.line_id, p.line_id) IS NOT NULL
),

enriched AS (
  SELECT
    c.*,
    l.line_name      AS line_name_dim,
    l.transport_mode AS transport_mode_dim,
    l.operator       AS operator_dim,

    -- MoM growth — correctly partitioned by line_id
    LAG(c.total_validations, 1) OVER (
      PARTITION BY c.line_id ORDER BY c.month_date
    ) AS prev_month_validations,

    SAFE_DIVIDE(
      c.total_validations - LAG(c.total_validations, 1) OVER (
        PARTITION BY c.line_id ORDER BY c.month_date
      ),
      NULLIF(LAG(c.total_validations, 1) OVER (
        PARTITION BY c.line_id ORDER BY c.month_date
      ), 0)
    ) * 100 AS mom_validation_growth_pct

  FROM combined c
  LEFT JOIN {{ ref('dim_line') }} AS l ON c.line_id = l.line_id
)

SELECT
  month_date,
  line_id,
  COALESCE(line_name,      line_name_dim)      AS line_name,
  COALESCE(transport_mode, transport_mode_dim) AS transport_mode,
  COALESCE(operator,       operator_dim)       AS operator,
  total_validations,
  COALESCE(avg_daily_validations, 0)           AS avg_daily_validations,
  days_with_data,
  stops_count,
  mom_validation_growth_pct,
  punctuality_rate,
  quality_category,

  CASE
    WHEN total_validations < COALESCE(prev_month_validations, total_validations) * 0.9
      AND (punctuality_rate IS NULL OR punctuality_rate < 85.0)
      THEN 'high_risk'

    WHEN total_validations < COALESCE(prev_month_validations, total_validations) * 0.95
      OR  (punctuality_rate IS NOT NULL AND punctuality_rate < 90.0)
      THEN 'medium_risk'

    ELSE 'low_risk'
  END AS risk_category,

  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM enriched
