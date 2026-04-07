{{
  config(
    materialized='table',
    description='Monthly station volume stability profiles. Grain: 1 station x 1 month (top stations only). 3 profiles based on coefficient of variation: high_volume_stable, high_volume_volatile, low_volume_erratic.'
  )
}}

-- V3 3p: mart_stop_volume_profile
-- Business question: Which stations show stable vs volatile demand patterns?
--
-- Design decisions:
--   - Grain: station_id_zdc only (not stop_id) — station-level is the right analytical unit
--   - Filtered to stations above minimum volume threshold — 1240 stations is too many
--     to be useful; focus on stations with meaningful traffic
--   - 3 profiles only (seasonal excluded — insufficient history 2023-2025)
--   - Coefficient of variation (CV = std/mean) is the right stability metric:
--     normalises variance by scale so high-volume and low-volume stations are comparable
--
-- Source: fct_validations_daily (station_id_zdc, daily_validation_count)

WITH monthly_station AS (
  SELECT
    DATE_TRUNC(validation_date, MONTH)  AS validation_month,
    station_id_zdc,
    stop_name,
    SUM(validation_count)               AS monthly_validations
  FROM {{ ref('fct_validations_daily') }}
  WHERE station_id_zdc IS NOT NULL
  GROUP BY 1, 2, 3
),

-- Minimum volume threshold: stations with avg < 1000 validations/month excluded
-- These stations have too little data for meaningful profile classification
station_avg AS (
  SELECT
    station_id_zdc,
    AVG(monthly_validations) AS avg_monthly_validations
  FROM monthly_station
  GROUP BY 1
),

with_stats AS (
  SELECT
    m.validation_month,
    m.station_id_zdc,
    m.stop_name,
    m.monthly_validations,
    s.avg_monthly_validations,

    -- Rolling 3-month average
    AVG(m.monthly_validations) OVER (
      PARTITION BY m.station_id_zdc
      ORDER BY m.validation_month
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_3m,

    -- Rolling 3-month std dev
    STDDEV(m.monthly_validations) OVER (
      PARTITION BY m.station_id_zdc
      ORDER BY m.validation_month
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_std_3m

  FROM monthly_station m
  INNER JOIN station_avg s ON m.station_id_zdc = s.station_id_zdc
  WHERE s.avg_monthly_validations >= 1000  -- minimum volume filter
)

SELECT
  validation_month,
  station_id_zdc,
  stop_name,
  monthly_validations,
  avg_monthly_validations,
  ROUND(rolling_avg_3m, 0)  AS rolling_avg_3m,
  ROUND(rolling_std_3m, 0)  AS rolling_std_3m,

  -- Coefficient of variation (CV = std / mean)
  -- Low CV (<0.15) = stable, High CV (>0.30) = volatile/erratic
  ROUND(
    CASE
      WHEN rolling_avg_3m = 0 OR rolling_avg_3m IS NULL THEN NULL
      ELSE rolling_std_3m / rolling_avg_3m
    END,
    3
  ) AS cv,

  -- Volume tier (based on long-term average)
  CASE
    WHEN avg_monthly_validations >= 50000 THEN 'high_volume'
    ELSE 'moderate_volume'
  END AS volume_tier,

  -- Stop profile — 3 types based on CV threshold
  CASE
    WHEN avg_monthly_validations >= 50000
      AND COALESCE(rolling_std_3m / NULLIF(rolling_avg_3m, 0), 0) < 0.15
      THEN 'high_volume_stable'

    WHEN avg_monthly_validations >= 50000
      AND COALESCE(rolling_std_3m / NULLIF(rolling_avg_3m, 0), 0) >= 0.15
      THEN 'high_volume_volatile'

    WHEN COALESCE(rolling_std_3m / NULLIF(rolling_avg_3m, 0), 0) >= 0.30
      THEN 'low_volume_erratic'

    ELSE 'moderate_stable'
  END AS stop_profile,

  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM with_stats
ORDER BY avg_monthly_validations DESC, validation_month
