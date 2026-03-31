{{
  config(
    materialized='table',
    description='Daily validations aggregated by station with coordinates — Looker Studio map. Joins via station_id_zdc=id_ref_zdc.'
  )
}}

-- Grain: 1 station x 1 day
-- station_id_zdc propagated from raw_validations.ida through stg → fct

WITH validations AS (
  SELECT
    validation_date,
    station_id_zdc,
    stop_name,
    SUM(validation_count) AS daily_validation_count
  FROM {{ ref("fct_validations_daily") }}
  WHERE station_id_zdc IS NOT NULL
  GROUP BY validation_date, station_id_zdc, stop_name
),

stations AS (
  SELECT
    station_id,
    station_name,
    latitude,
    longitude,
    transport_mode,
    operator
  FROM {{ ref("stg_ref_stations") }}
)

SELECT
  v.validation_date,
  v.station_id_zdc,
  v.stop_name,
  s.station_name,
  s.latitude,
  s.longitude,
  s.transport_mode,
  s.operator,
  v.daily_validation_count
FROM validations v
INNER JOIN stations s ON v.station_id_zdc = s.station_id
