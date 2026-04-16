-- Staging - Hourly Validation Profiles (ferré)
-- DISABLED: set enabled=True to activate.
-- ✅ READY TO ENABLE: raw_hourly_profiles is provisioned (transport_quarterly_pipeline loaded it 2026-04-15).
-- To enable: set enabled=True here only (no downstream model depends on stg_hourly_profiles yet — V4-A backlog).
{{
  config(
    materialized='view',
    enabled=True,
    description='Hourly validation profiles for rail network — cleaned and normalised. Grain: 1 stop × 1 hour (0-23) × 1 day_type (JOHV/SAM/DIM). Full snapshot — one edition per quarter.'
  )
}}

-- X5 enrichment layer: intra-day demand distribution by stop and day type.
-- Source: IDFM "Validations sur le réseau ferré : Profils horaires par jour type"
-- Published quarterly. Enables:
--   - Peak vs off-peak analysis
--   - Hourly demand allocation per line (V4-A weighted allocation)
--   - Enrichment of mart_validations_line_daily with time-of-day profiles
--
-- Raw day_type values: JOHV/JOVS (weekdays), SAHV/SAVS (saturdays), DIJFP (sundays/holidays).
-- Extractor normalises to JOHV / SAM / DIM before loading.
-- Deduplication: keeps most recently ingested row per (stop_id, hour, day_type).
-- Validation_share should sum to ~100% per (stop_id, day_type) across 24h.

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_hourly_profiles') }}
),

cleaned AS (
  SELECT
    CAST(stop_id AS STRING)                                    AS stop_id,
    TRIM(stop_name)                                            AS stop_name,
    CAST(line_code_trns AS STRING)                             AS line_code_trns,
    CAST(line_code_res AS STRING)                              AS line_code_res,
    CAST(hour AS INT64)                                        AS hour,
    UPPER(TRIM(day_type))                                      AS day_type,
    CAST(validation_share AS FLOAT64)                          AS validation_share,
    TRIM(quarter)                                              AS quarter,
    CAST(ingestion_ts AS TIMESTAMP)                            AS ingestion_ts,
    source
  FROM source
  WHERE
    stop_id IS NOT NULL
    AND hour IS NOT NULL
    AND day_type IS NOT NULL
    AND CAST(hour AS INT64) BETWEEN 0 AND 23
    AND UPPER(TRIM(day_type)) IN ('JOHV', 'SAM', 'DIM')
    AND CAST(validation_share AS FLOAT64) >= 0
),

deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY stop_id, hour, day_type
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM cleaned
),

-- Peak hour classification (Île-de-France rail standard peaks)
with_peak_flag AS (
  SELECT
    *,
    CASE
      WHEN day_type = 'JOHV' AND hour BETWEEN 7 AND 9    THEN 'morning_peak'
      WHEN day_type = 'JOHV' AND hour BETWEEN 17 AND 19  THEN 'evening_peak'
      WHEN day_type = 'JOHV' AND hour BETWEEN 6 AND 21   THEN 'daytime'
      WHEN day_type IN ('SAM', 'DIM') AND hour BETWEEN 10 AND 19 THEN 'daytime'
      WHEN hour BETWEEN 0 AND 4                           THEN 'night'
      ELSE 'off_peak'
    END                                                    AS time_period
  FROM deduplicated
  WHERE rn = 1
)

SELECT
  stop_id,
  stop_name,
  line_code_trns,
  line_code_res,
  hour,
  day_type,
  time_period,
  ROUND(validation_share, 4)                               AS validation_share,
  quarter,
  ingestion_ts,
  source
FROM with_peak_flag
