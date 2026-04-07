{{
  config(
    materialized='table',
    description='Monthly punctuality gap vs contractual target by line. Grain: 1 line x 1 month. Replaces fixed quality_category thresholds with per-line contractual objectives.'
  )
}}

-- V3 3g: mart_punctuality_vs_target
-- Business question: Which lines are persistently missing their contractual target?
--   quality_category uses the same thresholds for all lines (excellent≥95, good≥90).
--   But RER A target is 94% while Transilien K target is 88% — same score means
--   very different compliance. This mart gives a per-line contractual reading.
--
-- Source: fct_punctuality_monthly + line_punctuality_target seed
-- Pattern: Kenya Energy — gap_to_target + rolling avg, Flights — heavy_delay_probability

WITH punctuality AS (
  SELECT
    p.month_date,
    p.line_id,
    p.punctuality_rate,
    p.quality_category,
    t.line_name         AS target_line_name,
    t.target_punctuality_rate,
    t.operator
  FROM {{ ref('fct_punctuality_monthly') }} p
  LEFT JOIN {{ ref('line_punctuality_target') }} t
    ON UPPER(TRIM(p.line_id)) = UPPER(TRIM(t.line_id))
),

enriched AS (
  SELECT
    month_date,
    line_id,
    target_line_name,
    operator,
    punctuality_rate,
    target_punctuality_rate,
    quality_category,

    -- Gap to contractual target (positive = above target, negative = below)
    ROUND(punctuality_rate - target_punctuality_rate, 2) AS gap_to_target,

    -- 3-month rolling average (smooths month-to-month noise)
    ROUND(
      AVG(punctuality_rate) OVER (
        PARTITION BY line_id
        ORDER BY month_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ),
      2
    ) AS rolling_3m_avg,

    -- Rolling gap to target
    ROUND(
      AVG(punctuality_rate) OVER (
        PARTITION BY line_id
        ORDER BY month_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ) - target_punctuality_rate,
      2
    ) AS rolling_3m_gap_to_target,

    -- Count of months below target in last 12 (structural weakness signal)
    SUM(
      CASE WHEN punctuality_rate < target_punctuality_rate THEN 1 ELSE 0 END
    ) OVER (
      PARTITION BY line_id
      ORDER BY month_date
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ) AS months_below_target_12m

  FROM punctuality
  WHERE target_punctuality_rate IS NOT NULL  -- only lines with defined targets
)

SELECT
  month_date,
  line_id,
  target_line_name,
  operator,
  punctuality_rate,
  target_punctuality_rate,
  quality_category,
  gap_to_target,
  rolling_3m_avg,
  rolling_3m_gap_to_target,
  months_below_target_12m,

  -- Compliance status (more meaningful than generic quality_category)
  CASE
    WHEN target_punctuality_rate IS NULL      THEN 'no_target'
    WHEN gap_to_target >= 2.0                 THEN 'above'
    WHEN gap_to_target >= 0.0                 THEN 'near'
    WHEN gap_to_target >= -2.0                THEN 'below'
    ELSE                                           'critical'
  END AS target_status,

  -- Structural fragility flag (below target 6+ months out of last 12)
  CASE
    WHEN months_below_target_12m >= 6 THEN TRUE
    ELSE FALSE
  END AS structurally_fragile,

  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM enriched
ORDER BY month_date, line_id
