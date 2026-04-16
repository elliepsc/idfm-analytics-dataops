{{
  config(
    materialized='incremental',
    unique_key='strictness_check_key',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "metric_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['status', 'check_category', 'source_table'],
    description='Daily strictness monitor for non-blocking raw-data quality signals. Grain: 1 check x 1 day. Centralises soft failures that would otherwise stay split across Airflow logs, source tests, and ad hoc SQL checks.'
  )
}}

-- mart_strictness_monitor_daily
-- -----------------------------------------------------------------------------
-- Purpose
-- -----------------------------------------------------------------------------
-- This model turns "soft" pipeline issues into explicit daily monitoring rows.
-- Some problems do not fail the DAG or dbt build immediately:
--   - a RAW table exists but is empty
--   - a source test is configured as WARN instead of ERROR
--   - malformed values are filtered out downstream, so the pipeline stays green
--
-- Without a dedicated monitor, these signals remain scattered across:
--   - Airflow task logs
--   - dbt WARN summaries
--   - manual BigQuery inspection queries
--
-- This mart provides one canonical place to review those signals.
--
-- -----------------------------------------------------------------------------
-- Grain
-- -----------------------------------------------------------------------------
-- One row per (metric_date, check_id).
--
-- The model is incremental and partitioned by metric_date so it keeps daily
-- history. Each run overwrites the current day partition only.
--
-- -----------------------------------------------------------------------------
-- Scope of checks
-- -----------------------------------------------------------------------------
-- The first version focuses on the soft spots already observed in production:
--   1. raw_incidents_daily can exist but still be empty
--   2. raw_incidents_daily can contain NULL critical fields while source tests
--      remain non-blocking
--   3. raw_hourly_profiles can contain NULL hours (currently WARN only)
--   4. raw_service_quality can contain missing line identifiers or non-numeric
--      indicator values, which may be filtered out downstream
--
-- The design is intentionally extensible: new checks can be added as new UNION
-- ALL branches in the final checks CTE.

WITH incidents_stats AS (
  SELECT
    COUNT(*) AS row_count,
    COUNTIF(incident_date IS NULL) AS null_incident_date_count,
    COUNTIF(ingestion_ts IS NULL) AS null_ingestion_ts_count
  FROM {{ source('raw', 'raw_incidents_daily') }}
),

hourly_profiles_stats AS (
  SELECT
    COUNT(*) AS row_count,
    COUNTIF(hour IS NULL) AS null_hour_count
  FROM {{ source('raw', 'raw_hourly_profiles') }}
),

service_quality_stats AS (
  SELECT
    COUNT(*) AS row_count,
    COUNTIF(line_id IS NULL OR TRIM(line_id) = '') AS null_line_id_count,
    COUNTIF(
      indicator_value IS NULL
      OR TRIM(CAST(indicator_value AS STRING)) = ''
      OR SAFE_CAST(indicator_value AS FLOAT64) IS NULL
    ) AS invalid_indicator_value_count
  FROM {{ source('raw', 'raw_service_quality') }}
),

checks AS (
  SELECT
    CURRENT_DATE() AS metric_date,
    'availability' AS check_category,
    'raw_incidents_daily_non_empty' AS check_id,
    'raw_incidents_daily' AS source_table,
    NULL AS column_name,
    'error' AS expected_severity,
    CASE WHEN row_count > 0 THEN 'OK' ELSE 'ERROR' END AS status,
    CAST(row_count AS INT64) AS observed_value,
    1 AS expected_min_value,
    NULL AS expected_max_value,
    NULL AS failing_row_count,
    'raw_incidents_daily must contain at least one row. An empty table usually means the loader created the table but the extraction produced no usable incident file.' AS check_description,
    'Verify extract_incidents_daily logs, then confirm that incident files were produced and loaded into transport_raw.raw_incidents_daily.' AS recommendation
  FROM incidents_stats

  UNION ALL

  SELECT
    CURRENT_DATE() AS metric_date,
    'completeness' AS check_category,
    'raw_incidents_daily_null_incident_date' AS check_id,
    'raw_incidents_daily' AS source_table,
    'incident_date' AS column_name,
    'warn' AS expected_severity,
    CASE WHEN null_incident_date_count = 0 THEN 'OK' ELSE 'WARN' END AS status,
    CAST(null_incident_date_count AS INT64) AS observed_value,
    0 AS expected_min_value,
    0 AS expected_max_value,
    CAST(null_incident_date_count AS INT64) AS failing_row_count,
    'Tracks NULL incident_date values in the raw incidents feed. This mirrors the current source test, which is intentionally non-blocking.' AS check_description,
    'Inspect malformed incident payloads and confirm whether the upstream source omitted the incident start date.' AS recommendation
  FROM incidents_stats

  UNION ALL

  SELECT
    CURRENT_DATE() AS metric_date,
    'completeness' AS check_category,
    'raw_incidents_daily_null_ingestion_ts' AS check_id,
    'raw_incidents_daily' AS source_table,
    'ingestion_ts' AS column_name,
    'warn' AS expected_severity,
    CASE WHEN null_ingestion_ts_count = 0 THEN 'OK' ELSE 'WARN' END AS status,
    CAST(null_ingestion_ts_count AS INT64) AS observed_value,
    0 AS expected_min_value,
    0 AS expected_max_value,
    CAST(null_ingestion_ts_count AS INT64) AS failing_row_count,
    'Tracks NULL ingestion_ts values in the raw incidents feed. Missing ingestion metadata makes freshness auditing less reliable.' AS check_description,
    'Confirm that the extractor always stamps ingestion_ts before writing NDJSON output.' AS recommendation
  FROM incidents_stats

  UNION ALL

  SELECT
    CURRENT_DATE() AS metric_date,
    'validity' AS check_category,
    'raw_hourly_profiles_null_hour' AS check_id,
    'raw_hourly_profiles' AS source_table,
    'hour' AS column_name,
    'warn' AS expected_severity,
    CASE WHEN null_hour_count = 0 THEN 'OK' ELSE 'WARN' END AS status,
    CAST(null_hour_count AS INT64) AS observed_value,
    0 AS expected_min_value,
    0 AS expected_max_value,
    CAST(null_hour_count AS INT64) AS failing_row_count,
    'Tracks rows where the hourly profile extractor could not parse the hour label into a 0-23 integer.' AS check_description,
    'Review unexpected trnc_horr_60 formats in the quarterly extractor logs and extend the parser if new formats appear.' AS recommendation
  FROM hourly_profiles_stats

  UNION ALL

  SELECT
    CURRENT_DATE() AS metric_date,
    'completeness' AS check_category,
    'raw_service_quality_null_line_id' AS check_id,
    'raw_service_quality' AS source_table,
    'line_id' AS column_name,
    'warn' AS expected_severity,
    CASE WHEN null_line_id_count = 0 THEN 'OK' ELSE 'WARN' END AS status,
    CAST(null_line_id_count AS INT64) AS observed_value,
    0 AS expected_min_value,
    0 AS expected_max_value,
    CAST(null_line_id_count AS INT64) AS failing_row_count,
    'Tracks missing or blank line identifiers in the quarterly service quality snapshot. Some rows may still be recoverable through line_name fallback downstream.' AS check_description,
    'Audit source rows with missing line_id and confirm whether they are legitimate fallback cases or extractor mapping gaps.' AS recommendation
  FROM service_quality_stats

  UNION ALL

  SELECT
    CURRENT_DATE() AS metric_date,
    'validity' AS check_category,
    'raw_service_quality_invalid_indicator_value' AS check_id,
    'raw_service_quality' AS source_table,
    'indicator_value' AS column_name,
    'error' AS expected_severity,
    CASE WHEN invalid_indicator_value_count = 0 THEN 'OK' ELSE 'ERROR' END AS status,
    CAST(invalid_indicator_value_count AS INT64) AS observed_value,
    0 AS expected_min_value,
    0 AS expected_max_value,
    CAST(invalid_indicator_value_count AS INT64) AS failing_row_count,
    'Tracks missing or non-numeric indicator values in raw_service_quality. These rows are filtered out in staging, so the fact table can stay green while coverage silently degrades.' AS check_description,
    'Inspect the quarterly raw snapshot and fix unexpected indicator_value formats before the next rebuild of fct_service_quality_quarterly.' AS recommendation
  FROM service_quality_stats
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['metric_date', 'check_id']) }} AS strictness_check_key,
  metric_date,
  check_category,
  check_id,
  source_table,
  column_name,
  expected_severity,
  status,
  CASE status
    WHEN 'OK' THEN 1
    WHEN 'WARN' THEN 2
    ELSE 3
  END AS status_order,
  observed_value,
  expected_min_value,
  expected_max_value,
  failing_row_count,
  check_description,
  recommendation,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM checks
