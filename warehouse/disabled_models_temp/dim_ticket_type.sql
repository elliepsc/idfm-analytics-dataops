-- Dimension - Types de titres de transport
-- ================================================================

{{
  config(
    materialized='table',
    description='Dimension des types de titres de transport'
  )
}}

WITH ticket_types AS (
  SELECT DISTINCT
    ticket_type
  FROM {{ ref('stg_validations_rail_daily') }}
  WHERE ticket_type IS NOT NULL
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['ticket_type']) }} AS ticket_type_id,
  ticket_type AS ticket_type_name,
  CURRENT_TIMESTAMP() AS updated_at
FROM ticket_types
