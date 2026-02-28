{{
  config(
    materialized='table',
    description='Data health metrics for fct_validations_daily (freshness, volume, nulls, duplicates)'
  )
}}

-- FIX V2: file was truncated in V1 — started mid-CTE with no WITH or config block.

WITH validations_metrics AS (
  SELECT
    'fct_validations_daily' AS table_name,
    CURRENT_DATE() AS metric_date,
    COUNT(*) AS row_count,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingestion_ts), HOUR) AS freshness_hours,
    ROUND(100.0 * AVG(CASE WHEN stop_id IS NULL THEN 1.0 ELSE 0.0 END), 2) AS null_percentage,
    SUM(CASE WHEN dup_cnt > 1 THEN dup_cnt - 1 ELSE 0 END) AS duplicate_count
  FROM (
    SELECT
      *,
      COUNT(*) OVER (
        PARTITION BY stop_id, validation_date, ticket_type
      ) AS dup_cnt
    FROM {{ ref('fct_validations_daily') }}
    WHERE validation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  )
)

SELECT * FROM validations_metrics
