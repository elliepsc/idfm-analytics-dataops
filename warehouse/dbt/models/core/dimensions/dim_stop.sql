-- Dimension - Stops

{{
  config(
    materialized='table',
    description='Stop reference data from the source system (grain = 1 unique stop). Enriched with transport_mode, operator, insee_code from arrets-lignes export.'
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
  insee_code,
  transport_mode,
  operator,
  CURRENT_TIMESTAMP() AS updated_at
FROM stops
