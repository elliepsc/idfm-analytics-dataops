-- Dimension - Lignes

{{
  config(
    materialized='table',
    description='Dimension des lignes (grain = 1 ligne unique)'
  )
}}

WITH lines AS (
  SELECT * FROM {{ ref('stg_ref_lines') }}
)

SELECT
  line_id,
  line_name,
  transport_mode,
  operator,
  CURRENT_TIMESTAMP() AS updated_at
FROM lines
