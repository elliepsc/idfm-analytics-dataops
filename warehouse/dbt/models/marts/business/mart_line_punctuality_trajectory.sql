{{
  config(
    materialized='table',
    description='Monthly punctuality trajectory profiles by line. Grain: 1 line x 1 month. Transforms time series into behavioural typologies: resilient_corridor, structurally_fragile, recovering, chronically_weak, volatile_under_pressure.'
  )
}}

-- V3 3n: mart_line_punctuality_trajectory
-- Business questions:
--   1. Which lines are structurally fragile vs resilient corridors?
--   2. Which lines are recovering vs chronically weak?
--   3. Which lines are volatile under pressure vs consistently stable?
--
-- Key design principle: produce TYPOLOGIES, not flags.
--   "resilient_corridor" is more interpretable than "stable=TRUE".
--   These labels are the difference between a monitoring dashboard
--   and an analytically memorable output.
--
-- Source: fct_punctuality_monthly (~36 months 2023-2025)
-- Scope: Transilien/RER lines only (scope of punctuality data source)
-- Pattern: interpretive analytics — trajectories not levels

WITH monthly AS (
  SELECT
    month_date,
    line_id,
    punctuality_rate,
    quality_category,

    -- Rolling averages for smoothing
    AVG(punctuality_rate) OVER (
      PARTITION BY line_id
      ORDER BY month_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_3m,

    AVG(punctuality_rate) OVER (
      PARTITION BY line_id
      ORDER BY month_date
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_6m,

    -- Bad month flag (below 90% — acceptable threshold)
    CASE WHEN punctuality_rate < 90.0 THEN 1 ELSE 0 END AS bad_month_flag,

    -- Volatility: standard deviation over 6 months
    STDDEV(punctuality_rate) OVER (
      PARTITION BY line_id
      ORDER BY month_date
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS volatility_6m

  FROM {{ ref('fct_punctuality_monthly') }}
),

with_windows AS (
  SELECT
    month_date,
    line_id,
    punctuality_rate,
    quality_category,
    rolling_avg_3m,
    rolling_avg_6m,
    bad_month_flag,
    ROUND(volatility_6m, 2) AS volatility_6m,

    -- Bad months in last 3 and 6 (structural weakness signal)
    SUM(bad_month_flag) OVER (
      PARTITION BY line_id
      ORDER BY month_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS bad_months_last_3,

    SUM(bad_month_flag) OVER (
      PARTITION BY line_id
      ORDER BY month_date
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS bad_months_last_6,

    -- Recovery flag: bad last 3 months BUT improving vs 6m average
    CASE
      WHEN SUM(bad_month_flag) OVER (
        PARTITION BY line_id ORDER BY month_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ) >= 1
      AND punctuality_rate > AVG(punctuality_rate) OVER (
        PARTITION BY line_id ORDER BY month_date
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
      )
      THEN TRUE
      ELSE FALSE
    END AS recovery_flag

  FROM monthly
)

SELECT
  month_date,
  line_id,
  punctuality_rate,
  quality_category,
  rolling_avg_3m,
  rolling_avg_6m,
  volatility_6m,
  bad_months_last_3,
  bad_months_last_6,
  recovery_flag,

  -- Core interpretive output: trajectory typology
  -- These are BUSINESS PHENOMEMA, not technical flags
  CASE
    -- Chronically weak: bad 4+ months out of last 6
    WHEN bad_months_last_6 >= 4
      THEN 'chronically_weak'

    -- Structurally fragile: bad 2-3 months out of last 6 AND not recovering
    WHEN bad_months_last_6 >= 2 AND NOT recovery_flag
      THEN 'structurally_fragile'

    -- Recovering: had bad months recently but trending upward
    WHEN bad_months_last_3 >= 1 AND recovery_flag
      THEN 'recovering'

    -- Volatile under pressure: high volatility even if average is acceptable
    WHEN volatility_6m >= 2.5
      THEN 'volatile_under_pressure'

    -- Resilient corridor: consistently good, low volatility
    WHEN bad_months_last_6 = 0 AND volatility_6m < 2.5
      THEN 'resilient_corridor'

    ELSE 'stable'
  END AS trajectory_label,

  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM with_windows
ORDER BY line_id, month_date
