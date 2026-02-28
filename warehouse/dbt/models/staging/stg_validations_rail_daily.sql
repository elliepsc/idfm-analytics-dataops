-- Staging - Rail network ticket validations
{{
  config(
    materialized='view',
    description='Rail network ticket validations - cleaned and normalized'
  )
}}

-- Source fields in raw_validations (confirmed via API 2026-02-27):
--   date, stop_id, stop_name, line_code_trns, line_code_res, ticket_type, validation_count
--
-- FIX V2: removed line_id and line_name — these fields do NOT exist in raw_validations.
-- Line identity is reconstructed in fct_validations_daily via JOIN on dim_stop → dim_line.
-- FIX V2: field 'date' is already DATE type in BigQuery — PARSE_DATE() only works on STRING.
-- FIX V2: line_code_trns and line_code_res are INT64 — TRIM() requires STRING, cast first.

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_validations') }}
),

cleaned AS (
  SELECT
    -- Date (already DATE type — no PARSE_DATE needed)
    date AS validation_date,
    DATE_TRUNC(date, MONTH) AS validation_month,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,

    -- Stop identifier
    UPPER(TRIM(stop_id)) AS stop_id,
    TRIM(stop_name) AS stop_name,

    -- Line codes (INT64 in BigQuery — cast to STRING before TRIM/UPPER)
    UPPER(TRIM(CAST(line_code_trns AS STRING))) AS line_code_trns,
    UPPER(TRIM(CAST(line_code_res AS STRING))) AS line_code_res,

    -- Ticket category
    UPPER(TRIM(ticket_type)) AS ticket_type,

    -- Metrics
    CAST(validation_count AS INT64) AS validation_count,

    -- Metadata
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
    source

  FROM source

  WHERE
    date IS NOT NULL
    AND stop_id IS NOT NULL
    AND validation_count IS NOT NULL
    AND CAST(validation_count AS INT64) >= 0
)

SELECT * FROM cleaned
