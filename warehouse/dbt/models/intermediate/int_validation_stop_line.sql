{{
  config(
    materialized='view',
    schema='staging',
    description='Bridge model from validation stops (STIF codes) to IDFM lines with mapping confidence.'
  )
}}

-- Bridge validation rows from STIF stop codes to IDFM lines.
-- This model is intentionally conservative:
--   - "high" confidence: one mapped line only
--   - "medium" confidence: multiple lines, same transport mode
--   - "low" confidence: multimodal or ambiguous hub
-- Downstream facts should start with high-confidence mappings only.

WITH validations AS (
  SELECT
    validation_date,
    validation_month,
    stop_id AS stif_stop_id,
    stop_name AS stop_name_validation,
    station_id_zdc,
    ticket_type,
    validation_count,
    ingestion_ts
  FROM {{ ref('fct_validations_daily') }}
),

stop_id_bridge AS (
  SELECT
    stif_stop_code,
    idfm_stop_id
  FROM {{ ref('stg_ref_stop_id_mapping') }}
),

stop_line_candidates AS (
  SELECT
    sl.stop_id AS idfm_stop_id,
    sl.line_id,
    l.line_name,
    l.transport_mode,
    l.operator,
    sl.ingestion_ts
  FROM {{ ref('stg_ref_stop_lines') }} sl
  INNER JOIN {{ ref('stg_ref_lines') }} l
    ON sl.line_id = l.line_id
),

stop_reference AS (
  SELECT
    stop_id,
    stop_name AS stop_name_ref
  FROM {{ ref('stg_ref_stops') }}
),

bridged AS (
  SELECT
    v.validation_date,
    v.validation_month,
    v.stif_stop_id,
    b.idfm_stop_id,
    v.station_id_zdc,
    v.stop_name_validation,
    sr.stop_name_ref,
    c.line_id,
    c.line_name,
    c.transport_mode,
    c.operator,
    v.ticket_type,
    v.validation_count,
    v.ingestion_ts AS validation_ingestion_ts,
    c.ingestion_ts AS stop_line_ingestion_ts
  FROM validations v
  LEFT JOIN stop_id_bridge b
    ON v.stif_stop_id = b.stif_stop_code
  LEFT JOIN stop_line_candidates c
    ON b.idfm_stop_id = c.idfm_stop_id
  LEFT JOIN stop_reference sr
    ON b.idfm_stop_id = sr.stop_id
),

candidate_stats AS (
  SELECT
    stif_stop_id,
    idfm_stop_id,
    COUNT(DISTINCT line_id) AS candidate_line_count,
    COUNT(DISTINCT transport_mode) AS candidate_mode_count
  FROM bridged
  WHERE line_id IS NOT NULL
  GROUP BY 1, 2
),

same_mode_stats AS (
  SELECT
    stif_stop_id,
    idfm_stop_id,
    transport_mode,
    COUNT(DISTINCT line_id) AS same_mode_line_count
  FROM bridged
  WHERE line_id IS NOT NULL
  GROUP BY 1, 2, 3
)

SELECT
  {{ dbt_utils.generate_surrogate_key([
    'b.validation_date',
    'b.stif_stop_id',
    'b.idfm_stop_id',
    'b.ticket_type',
    'b.line_id'
  ]) }} AS validation_bridge_key,

  b.validation_date,
  b.validation_month,
  b.stif_stop_id,
  b.idfm_stop_id,
  b.station_id_zdc,
  b.stop_name_validation,
  b.stop_name_ref,
  b.line_id,
  b.line_name,
  b.transport_mode,
  b.operator,
  b.ticket_type,
  b.validation_count,

  COALESCE(cs.candidate_line_count, 0) AS candidate_line_count,
  COALESCE(sm.same_mode_line_count, 0) AS same_mode_line_count,
  COALESCE(cs.candidate_line_count, 0) = 1 AS is_mono_line_stop,

  CASE
    WHEN line_id IS NULL THEN 'unmapped'
    WHEN COALESCE(cs.candidate_line_count, 0) = 1 THEN 'high'
    WHEN COALESCE(cs.candidate_mode_count, 0) = 1 THEN 'medium'
    ELSE 'low'
  END AS mapping_confidence,

  CASE
    WHEN line_id IS NULL THEN NULL
    WHEN COALESCE(cs.candidate_line_count, 0) = 1 THEN 1.0
    ELSE NULL
  END AS allocation_weight,

  CASE
    WHEN line_id IS NOT NULL AND COALESCE(cs.candidate_line_count, 0) = 1
      THEN TRUE
    ELSE FALSE
  END AS is_trusted_mapping,

  'gtfs_tnpa_bridge' AS mapping_source,
  GREATEST(
    validation_ingestion_ts,
    COALESCE(stop_line_ingestion_ts, validation_ingestion_ts)
  ) AS ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM bridged b
LEFT JOIN candidate_stats cs
  ON b.stif_stop_id = cs.stif_stop_id
 AND b.idfm_stop_id = cs.idfm_stop_id
LEFT JOIN same_mode_stats sm
  ON b.stif_stop_id = sm.stif_stop_id
 AND b.idfm_stop_id = sm.idfm_stop_id
 AND b.transport_mode = sm.transport_mode
