{{
  config(
    materialized='table',
    description='Monthly Navigo subscription penetration rate by station — KPI for network loyalty. Grain: 1 station x 1 month.'
  )
}}

-- V3 3c: mart_navigo_penetration_rate
-- Business question: Which stations have the highest share of subscription riders?
--   High navigo_penetration_rate → captive, loyal ridership
--   Low rate → mostly occasional / tourist traffic
--
-- Source: fct_validations_daily (is_subscription flag from 3a)
--         stg_ref_stations (station name + coordinates)
-- Pattern: Steam Video Games — positive_ratio null-safe, seed ticket_type_mapping existante

WITH validations_monthly AS (
  SELECT
    DATE_TRUNC(validation_date, MONTH) AS validation_month,
    station_id_zdc,
    stop_name,
    SUM(validation_count)                                              AS total_validations,
    SUM(CASE WHEN is_subscription THEN validation_count ELSE 0 END)   AS subscription_validations,
    SUM(CASE WHEN NOT is_subscription THEN validation_count ELSE 0 END) AS occasional_validations
  FROM {{ ref('fct_validations_daily') }}
  WHERE station_id_zdc IS NOT NULL
  GROUP BY 1, 2, 3
),

stations AS (
  SELECT
    station_id,
    station_name,
    transport_mode,
    operator
  FROM {{ ref('stg_ref_stations') }}
),

enriched AS (
  SELECT
    v.validation_month,
    v.station_id_zdc,
    v.stop_name,
    s.station_name,
    s.transport_mode,
    s.operator,
    v.total_validations,
    v.subscription_validations,
    v.occasional_validations,

    -- Penetration rates (null-safe via safe_divide)
    ROUND(
      {{ safe_divide('v.subscription_validations', 'v.total_validations') }} * 100,
      2
    ) AS navigo_penetration_rate,

    ROUND(
      {{ safe_divide('v.occasional_validations', 'v.total_validations') }} * 100,
      2
    ) AS occasional_rate,

    -- Loyalty segment
    CASE
      WHEN {{ safe_divide('v.subscription_validations', 'v.total_validations') }} >= 0.8
        THEN 'high_loyalty'
      WHEN {{ safe_divide('v.subscription_validations', 'v.total_validations') }} >= 0.5
        THEN 'mixed'
      ELSE 'occasional_dominant'
    END AS loyalty_segment

  FROM validations_monthly v
  LEFT JOIN stations s ON v.station_id_zdc = s.station_id
)

SELECT
  validation_month,
  station_id_zdc,
  stop_name,
  station_name,
  transport_mode,
  operator,
  total_validations,
  subscription_validations,
  occasional_validations,
  navigo_penetration_rate,
  occasional_rate,
  loyalty_segment,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM enriched
WHERE total_validations > 0
