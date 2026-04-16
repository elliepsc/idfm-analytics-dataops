{{
  config(
    materialized='table',
    description='Year-over-year monthly validation comparison (N vs N-1). Grain: 1 month. LAG(12) gives true seasonal baseline vs noisy MoM.'
  )
}}

-- V3 3e: mart_validations_yoy_monthly
-- Business question: How does this month compare to the same month last year?
--   LAG(12) eliminates seasonal noise present in MoM comparisons.
--   mart_network_scorecard_monthly uses LAG(1) — sensitive to school holidays,
--   strikes, seasonal patterns. YoY is the right baseline for trend detection.
--
-- Source: fct_validations_daily
-- Pattern: Kenya Energy — renewable_share_yoy_pp = LAG(1,12)

WITH monthly AS (
  SELECT
    validation_month,
    {{ fiscal_quarter('validation_month') }}    AS fiscal_quarter,
    EXTRACT(YEAR FROM validation_month)         AS year,
    EXTRACT(MONTH FROM validation_month)        AS month_num,
    SUM(validation_count)                       AS total_validations,
    COUNT(DISTINCT stop_id)                     AS active_stops,
    COUNT(DISTINCT DATE(validation_date))       AS days_with_data
  FROM {{ ref('fct_validations_daily') }}
  GROUP BY 1, 2, 3, 4
),

yoy AS (
  SELECT
    validation_month,
    fiscal_quarter,
    year,
    month_num,
    total_validations,
    active_stops,
    days_with_data,

    -- Same month last year (true seasonal baseline)
    LAG(total_validations, 12) OVER (
      ORDER BY validation_month
    ) AS same_month_last_year,

    -- YoY growth %
    -- NOTE: native SAFE_DIVIDE used (not macro) to avoid operator precedence issues
    ROUND(
      SAFE_DIVIDE(
        total_validations - LAG(total_validations, 12) OVER (ORDER BY validation_month),
        NULLIF(LAG(total_validations, 12) OVER (ORDER BY validation_month), 0)
      ) * 100,
      2
    ) AS yoy_growth_pct,

    -- MoM for reference (kept but secondary)
    LAG(total_validations, 1) OVER (
      ORDER BY validation_month
    ) AS prev_month_validations,

    ROUND(
      SAFE_DIVIDE(
        total_validations - LAG(total_validations, 1) OVER (ORDER BY validation_month),
        NULLIF(LAG(total_validations, 1) OVER (ORDER BY validation_month), 0)
      ) * 100,
      2
    ) AS mom_growth_pct

  FROM monthly
)

SELECT
  validation_month,
  fiscal_quarter,
  year,
  month_num,
  total_validations,
  active_stops,
  days_with_data,
  same_month_last_year,
  yoy_growth_pct,
  prev_month_validations,
  mom_growth_pct,

  -- YoY trend label
  CASE
    WHEN same_month_last_year IS NULL   THEN 'no_baseline'
    WHEN yoy_growth_pct >= 5.0          THEN 'strong_growth'
    WHEN yoy_growth_pct >= 1.0          THEN 'moderate_growth'
    WHEN yoy_growth_pct >= -1.0         THEN 'stable'
    WHEN yoy_growth_pct >= -5.0         THEN 'moderate_decline'
    ELSE                                     'strong_decline'
  END AS yoy_trend,

  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM yoy
ORDER BY validation_month
