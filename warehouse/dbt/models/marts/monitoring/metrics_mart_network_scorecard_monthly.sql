{{
  config(
    materialized='table',
    description='Data health metrics for mart_network_scorecard_monthly (freshness, volume, nulls, duplicates)'
  )
}}

-- FIX V2: file was truncated in V1 — started mid-CTE with no WITH or config block.

WITH scorecard_metrics AS (
  SELECT
    'mart_network_scorecard_monthly' AS table_name,
    CURRENT_DATE() AS metric_date,
    COUNT(*) AS row_count,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(dbt_updated_at), HOUR) AS freshness_hours,
    ROUND(100.0 * AVG(CASE WHEN line_id IS NULL THEN 1.0 ELSE 0.0 END), 2) AS null_percentage,
    SUM(CASE WHEN dup_cnt > 1 THEN dup_cnt - 1 ELSE 0 END) AS duplicate_count
  FROM (
    SELECT
      *,
      COUNT(*) OVER (
        PARTITION BY line_id, month_date
      ) AS dup_cnt
    FROM {{ ref('mart_network_scorecard_monthly') }}
    WHERE month_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
  )
)

SELECT * FROM scorecard_metrics
