{{
  config(
    materialized='table',
    description='Daily validations aggregated by station with coordinates — used for Looker Studio map'
  )
}}

-- Daily validation counts per station with geographic coordinates
-- Joins fct_validations_daily with dim_stop to add lat/lon for Looker Studio map
-- Grain: 1 station × 1 day

WITH validations AS (
  SELECT
    validation_date,
    stop_id,
    stop_name,
    SUM(validation_count) AS daily_validation_count
  FROM {{ ref('fct_validations_daily') }}
  GROUP BY validation_date, stop_id, stop_name
),

stops AS (
  SELECT
    stop_id,
    latitude,
    longitude,
    town
  FROM {{ ref('dim_stop') }}
  WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL
)

SELECT
  v.validation_date,
  v.stop_id,
  v.stop_name,
  s.latitude,
  s.longitude,
  s.town,
  v.daily_validation_count
FROM validations v
INNER JOIN stops s ON v.stop_id = s.stop_id
