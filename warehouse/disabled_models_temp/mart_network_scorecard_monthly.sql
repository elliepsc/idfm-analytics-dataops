-- Mart - Scorecard monthly : demand + quality per line and month

{{
  config(
    materialized='table',
    description='Scorecard réseau : demand + quality par ligne et par mois'
  )
}}

WITH validations_monthly AS (
  SELECT
    line_id,
    validation_month AS month_date,
    SUM(validation_count) AS total_validations,
    COUNT(DISTINCT validation_date) AS days_with_data,
    COUNT(DISTINCT stop_id) AS stops_count
  FROM {{ ref('fct_validations_daily') }}
  GROUP BY line_id, validation_month
),

punctuality_monthly AS (
  SELECT
    line_id,
    month_date,
    punctuality_rate,
    quality_category,
    trains_planned,
    trains_departed
  FROM {{ ref('fct_punctuality_monthly') }}
),

combined AS (
  SELECT
    COALESCE(v.month_date, p.month_date) AS month_date,
    COALESCE(v.line_id, p.line_id) AS line_id,

    -- Demand metrics
    v.total_validations,
    v.days_with_data,
    v.stops_count,
    SAFE_DIVIDE(v.total_validations, v.days_with_data) AS avg_daily_validations,

    -- Quality metrics
    p.punctuality_rate,
    p.quality_category,
    p.trains_planned,
    p.trains_departed,

  FROM validations_monthly v
  FULL OUTER JOIN punctuality_monthly p
    ON v.line_id = p.line_id
    AND v.month_date = p.month_date
),

enriched AS (
  SELECT
    c.*,
    l.line_name,
    l.transport_mode,
    l.operator,

    -- Month-over-month growth
    LAG(c.total_validations, 1) OVER (
      PARTITION BY c.line_id
      ORDER BY c.month_date
    ) AS prev_month_validations,

    SAFE_DIVIDE(
      c.total_validations - LAG(c.total_validations, 1) OVER (
        PARTITION BY c.line_id
        ORDER BY c.month_date
      ),
      LAG(c.total_validations, 1) OVER (
        PARTITION BY c.line_id
        ORDER BY c.month_date
      )
    ) * 100 AS mom_validation_growth_pct,

    -- Risk score (baisse demand + dégradation quality)
    CASE
      WHEN c.total_validations < LAG(c.total_validations, 1) OVER (
        PARTITION BY c.line_id ORDER BY c.month_date
      ) * 0.9  -- Baisse > 10%
      AND c.punctuality_rate < 85.0  -- Ponctualité < 85%
      THEN 'high_risk'

      WHEN c.total_validations < LAG(c.total_validations, 1) OVER (
        PARTITION BY c.line_id ORDER BY c.month_date
      ) * 0.95  -- Baisse > 5%
      OR c.punctuality_rate < 90.0
      THEN 'medium_risk'

      ELSE 'low_risk'
    END AS risk_category

  FROM combined c
  LEFT JOIN {{ ref('dim_line') }} l
    ON c.line_id = l.line_id
)

SELECT
  month_date,
  line_id,
  line_name,
  transport_mode,
  operator,

  -- Demand
  total_validations,
  avg_daily_validations,
  days_with_data,
  stops_count,
  mom_validation_growth_pct,

  -- Quality
  punctuality_rate,
  quality_category,
  trains_planned,
  trains_departed,

  -- Risk
  risk_category,

  -- Metadata
  CURRENT_TIMESTAMP() AS ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM enriched
