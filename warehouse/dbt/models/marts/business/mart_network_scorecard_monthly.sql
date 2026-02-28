{{
  config(
    materialized='table',
    description='Network scorecard: demand + quality per line and month'
  )
}}

-- FIX V2: removed line_id from fct_validations_daily JOIN — field does not exist in source.
-- Validations are aggregated at stop level; line context comes from fct_punctuality_monthly.
-- FIX V2: removed trains_planned and trains_departed — not in ponctualite-mensuelle-transilien.

WITH validations_monthly AS (
  SELECT
    -- FIX V2: no line_id available — aggregate at stop/month level only
    validation_month AS month_date,
    SUM(validation_count) AS total_validations,
    COUNT(DISTINCT validation_date) AS days_with_data,
    COUNT(DISTINCT stop_id) AS stops_count
  FROM {{ ref('fct_validations_daily') }}
  GROUP BY validation_month
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
    p.month_date,
    p.line_id,

    -- Demand metrics (network-wide, no line breakdown available from validations)
    v.total_validations,
    v.days_with_data,
    v.stops_count,
    SAFE_DIVIDE(v.total_validations, v.days_with_data) AS avg_daily_validations,

    -- Quality metrics
    p.punctuality_rate,
    p.quality_category

  FROM punctuality_monthly p
  LEFT JOIN validations_monthly v
    ON p.month_date = v.month_date
),

enriched AS (
  SELECT
    c.*,
    l.line_name,
    l.transport_mode,
    l.operator,

    -- Month-over-month growth
    LAG(c.total_validations, 1) OVER (
      ORDER BY c.month_date
    ) AS prev_month_validations,

    SAFE_DIVIDE(
      c.total_validations - LAG(c.total_validations, 1) OVER (
        ORDER BY c.month_date
      ),
      LAG(c.total_validations, 1) OVER (
        ORDER BY c.month_date
      )
    ) * 100 AS mom_validation_growth_pct,

    -- Risk score
    CASE
      WHEN c.total_validations < LAG(c.total_validations, 1) OVER (
        ORDER BY c.month_date
      ) * 0.9
      AND c.punctuality_rate < 85.0
      THEN 'high_risk'

      WHEN c.total_validations < LAG(c.total_validations, 1) OVER (
        ORDER BY c.month_date
      ) * 0.95
      OR c.punctuality_rate < 90.0
      THEN 'medium_risk'

      ELSE 'low_risk'
    END AS risk_category

  FROM combined c
  LEFT JOIN {{ ref('dim_line') }} l ON c.line_id = l.line_id
)

SELECT
  month_date,
  line_id,
  line_name,
  transport_mode,
  operator,
  total_validations,
  avg_daily_validations,
  days_with_data,
  stops_count,
  mom_validation_growth_pct,
  punctuality_rate,
  quality_category,
  risk_category,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM enriched
