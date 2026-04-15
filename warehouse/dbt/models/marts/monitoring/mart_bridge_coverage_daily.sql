{{
  config(
    materialized='table',
    description='Daily coverage KPIs for the validation stop→line bridge. Grain: 1 day. Audits % mapped, % trusted, % unmapped to prevent silent overstatement of metro line precision.'
  )
}}

-- Coverage is computed at the stop×date×ticket_type grain (validation unit).
-- Multi-line stops (medium/low confidence) appear N times in int_validation_stop_line
-- with the same validation_count — we deduplicate first to avoid double-counting.

WITH deduped AS (
  SELECT
    validation_date,
    stif_stop_id,
    ticket_type,
    -- validation_count is the same for all candidate lines of a stop×date×ticket_type
    ANY_VALUE(validation_count)                                    AS validation_count,
    MAX(CASE WHEN line_id IS NOT NULL THEN 1 ELSE 0 END)           AS has_mapping,
    MAX(CASE WHEN is_trusted_mapping THEN 1 ELSE 0 END)            AS is_trusted
  FROM {{ ref('int_validation_stop_line') }}
  GROUP BY validation_date, stif_stop_id, ticket_type
)

SELECT
  validation_date,

  -- Volume KPIs (validation-count weighted)
  SUM(validation_count)                                             AS total_validation_count,
  SUM(CASE WHEN has_mapping = 1 THEN validation_count ELSE 0 END)  AS mapped_validation_count,
  SUM(CASE WHEN is_trusted = 1  THEN validation_count ELSE 0 END)  AS trusted_validation_count,
  SUM(CASE WHEN has_mapping = 0 THEN validation_count ELSE 0 END)  AS unmapped_validation_count,

  -- Coverage ratios (%)
  ROUND(SAFE_DIVIDE(
    SUM(CASE WHEN has_mapping = 1 THEN validation_count ELSE 0 END),
    SUM(validation_count)
  ) * 100, 2) AS pct_mapped,

  ROUND(SAFE_DIVIDE(
    SUM(CASE WHEN is_trusted = 1 THEN validation_count ELSE 0 END),
    SUM(validation_count)
  ) * 100, 2) AS pct_trusted,

  ROUND(SAFE_DIVIDE(
    SUM(CASE WHEN has_mapping = 0 THEN validation_count ELSE 0 END),
    SUM(validation_count)
  ) * 100, 2) AS pct_unmapped,

  -- Stop-level KPIs (distinct stops)
  COUNT(DISTINCT stif_stop_id)                                      AS total_stop_count,
  COUNT(DISTINCT CASE WHEN has_mapping = 1 THEN stif_stop_id END)   AS mapped_stop_count,
  COUNT(DISTINCT CASE WHEN is_trusted = 1 THEN stif_stop_id END)    AS trusted_stop_count,

  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM deduped
GROUP BY validation_date
