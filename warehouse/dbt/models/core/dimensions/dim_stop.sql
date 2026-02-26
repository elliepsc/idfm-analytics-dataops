-- Dimension - Arrêts

{{
  config(
    materialized='table',
    description='Dimension des arrêts (grain = 1 arrêt unique)'
  )
}}

WITH stops AS (
  SELECT * FROM {{ ref('stg_ref_stops') }}
)

SELECT
  stop_id,
  stop_name,
  latitude,
  longitude,
  town,
  CURRENT_TIMESTAMP() AS updated_at
FROM stops
