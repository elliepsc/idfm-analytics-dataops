-- Mart - Data Health & SLA monitoring
{{
  config(
    materialized='table',
    description='Daily data health monitoring: freshness, volume, SLA compliance'
  )
}}

-- FIX V2: V1 file was truncated — SQL ended after sla_config CTE with no SELECT.
-- FIX V2: INFORMATION_SCHEMA.PARTITIONS does not have a 'row_count' column in BigQuery.
-- Using __TABLES__ system table which has 'row_count' and 'last_modified_time'.

WITH sla_config AS (
  SELECT 'fct_validations_daily'          AS table_name, 30       AS sla_hours UNION ALL
  SELECT 'fct_punctuality_monthly',        24 * 45                              UNION ALL
  SELECT 'mart_network_scorecard_monthly', 24 * 45
),

table_stats AS (
  SELECT
    table_id AS table_name,
    row_count,
    TIMESTAMP_DIFF(
      CURRENT_TIMESTAMP(),
      TIMESTAMP_MILLIS(last_modified_time),
      HOUR
    ) AS freshness_hours
  FROM `{{ env_var('GCP_PROJECT_ID') }}.transport_analytics.__TABLES__`
  WHERE table_id IN (
    'fct_validations_daily',
    'fct_punctuality_monthly',
    'mart_network_scorecard_monthly'
  )
)

SELECT
  sla.table_name,
  CURRENT_DATE() AS metric_date,
  COALESCE(t.row_count, 0) AS row_count,
  COALESCE(t.freshness_hours, 9999) AS freshness_hours,
  sla.sla_hours,
  (
    COALESCE(t.freshness_hours, 9999) <= sla.sla_hours
    AND COALESCE(t.row_count, 0) > 0
  ) AS sla_met
FROM sla_config sla
LEFT JOIN table_stats t ON sla.table_name = t.table_name
