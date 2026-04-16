-- X4 mart: enabled. fct_incidents_daily runs with 0 rows until PRIM API is configured (LEFT JOIN — no crash).
{{
  config(
    materialized='table',
    enabled=True,
    description='Monthly reliability scorecard per station. Grain: 1 station × 1 month. Combines station-level validations with network-wide incident KPIs. Useful for hub analysis (Châtelet, La Défense).'
  )
}}

-- X4 reliability layer (optional): station-level reliability scorecard.
--
-- Rationale from backlog: the station is a more robust unit than the line
-- in major interchange hubs (Châtelet, La Défense) where validations span
-- multiple lines but the physical experience is per-station.
--
-- Sources:
--   demand   → mart_validations_station_daily (all validated stops with lat/lon)
--   incidents → fct_incidents_daily (network-wide, month grain)
--
-- NOTE: service quality (fct_service_quality_quarterly) is LINE-level.
-- Until a stop→line mapping for service quality exists, it is excluded here.
-- Use mart_line_reliability_scorecard for quality-enriched analysis.
--
-- station_reliability_band:
--   high    → total_validations >= p75 of network AND incident pressure is low
--   medium  → otherwise
--   low     → total_validations <= p25 OR high incident pressure

WITH station_monthly AS (
  SELECT
    DATE_TRUNC(validation_date, MONTH)            AS month_date,
    station_id_zdc,
    stop_name,
    -- Aggregate across all days in the month
    SUM(daily_validation_count)                   AS total_validations,
    COUNT(DISTINCT validation_date)               AS days_with_data,
    ROUND(AVG(daily_validation_count), 0)         AS avg_daily_validations,
    -- Coordinates: take the most recent non-null value
    MAX(latitude)                                 AS latitude,
    MAX(longitude)                                AS longitude
  FROM {{ ref('mart_validations_station_daily') }}
  GROUP BY
    DATE_TRUNC(validation_date, MONTH),
    station_id_zdc,
    stop_name
),

-- MoM demand growth per station
station_with_growth AS (
  SELECT
    *,
    LAG(total_validations) OVER (
      PARTITION BY station_id_zdc
      ORDER BY month_date
    )                                             AS prev_month_validations,

    ROUND(
      SAFE_DIVIDE(
        total_validations - LAG(total_validations) OVER (PARTITION BY station_id_zdc ORDER BY month_date),
        NULLIF(LAG(total_validations) OVER (PARTITION BY station_id_zdc ORDER BY month_date), 0)
      ) * 100,
      2
    )                                             AS mom_growth_pct
  FROM station_monthly
),

-- Network-wide validation percentiles for reliability band classification
network_percentiles AS (
  SELECT
    month_date,
    APPROX_QUANTILES(total_validations, 4)[OFFSET(1)] AS p25_validations,
    APPROX_QUANTILES(total_validations, 4)[OFFSET(3)] AS p75_validations
  FROM station_monthly
  GROUP BY month_date
),

-- Incidents: monthly aggregate (network-wide)
incidents_monthly AS (
  SELECT
    incident_month                                AS month_date,
    SUM(incident_count)                           AS incident_count,
    SUM(major_incident_count)                     AS major_incident_count
  FROM {{ ref('fct_incidents_daily') }}
  GROUP BY incident_month
),

combined AS (
  SELECT
    s.month_date,
    s.station_id_zdc,
    s.stop_name,
    s.total_validations,
    s.days_with_data,
    s.avg_daily_validations,
    s.latitude,
    s.longitude,
    s.prev_month_validations,
    s.mom_growth_pct,
    p.p25_validations,
    p.p75_validations,
    COALESCE(i.incident_count, 0)                 AS incident_count,
    COALESCE(i.major_incident_count, 0)           AS major_incident_count
  FROM station_with_growth s
  LEFT JOIN network_percentiles p
    ON s.month_date = p.month_date
  LEFT JOIN incidents_monthly i
    ON s.month_date = i.month_date
)

SELECT
  month_date,
  station_id_zdc,
  stop_name,
  latitude,
  longitude,

  -- Demand
  total_validations,
  days_with_data,
  avg_daily_validations,
  prev_month_validations,
  mom_growth_pct,

  -- Network context
  p25_validations,
  p75_validations,

  -- Incidents (network-wide)
  incident_count,
  major_incident_count,

  -- Reliability band
  -- High: above p75 demand AND low incident pressure
  -- Low: below p25 demand OR high incident pressure
  CASE
    WHEN total_validations >= p75_validations AND major_incident_count <= 2
      THEN 'high'
    WHEN total_validations <= p25_validations OR major_incident_count >= 5
      THEN 'low'
    ELSE 'medium'
  END                                             AS station_reliability_band,

  CURRENT_TIMESTAMP()                             AS dbt_updated_at

FROM combined
ORDER BY month_date DESC, total_validations DESC
