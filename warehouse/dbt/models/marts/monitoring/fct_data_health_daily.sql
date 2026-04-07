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
  SELECT table_id AS table_name, row_count,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(last_modified_time), HOUR) AS freshness_hours
  FROM `{{ env_var('GCP_PROJECT_ID') }}.{{ target.dataset }}_core.__TABLES__`
  WHERE table_id IN ('fct_validations_daily', 'fct_punctuality_monthly')

  UNION ALL

  SELECT table_id AS table_name, row_count,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MILLIS(last_modified_time), HOUR) AS freshness_hours
  FROM `{{ env_var('GCP_PROJECT_ID') }}.{{ target.dataset }}_analytics.__TABLES__`
  WHERE table_id IN ('mart_network_scorecard_monthly')
)

SELECT
  sla.table_name,
  CURRENT_DATE() AS metric_date,
  COALESCE(t.row_count, 0)      AS row_count,
  COALESCE(t.freshness_hours, 9999) AS freshness_hours,
  sla.sla_hours,

  -- SLA respect (booleen — for dbt tests and airflow sensors)
  (
    COALESCE(t.freshness_hours, 9999) <= sla.sla_hours
    AND COALESCE(t.row_count, 0) > 0
  ) AS sla_met,

  -- SLA respect (numeric for Looker visualisation)
  CASE
    WHEN COALESCE(t.freshness_hours, 9999) <= sla.sla_hours THEN 1
    ELSE 0
  END AS sla_numeric,

  -- Delta freshness vs SLA (negative = under SLA, positive = breach)
  COALESCE(t.freshness_hours, 9999) - sla.sla_hours AS freshness_delta,

  -- Business status label
  CASE
    WHEN COALESCE(t.freshness_hours, 9999) <= sla.sla_hours        THEN 'OK'
    WHEN COALESCE(t.freshness_hours, 9999) <= sla.sla_hours * 1.2  THEN 'WARNING'
    ELSE 'CRITICAL'
  END AS sla_status_label,

  -- Freshness ratio vs SLA (0.5 = safe, 1.0 = at limit, >1 = breached)
  CASE
    WHEN sla.sla_hours = 0 THEN NULL
    ELSE COALESCE(t.freshness_hours, 9999) / sla.sla_hours
  END AS freshness_ratio,

  -- Sort order for Looker (1=OK, 2=WARNING, 3=CRITICAL)
  CASE
    WHEN COALESCE(t.freshness_hours, 9999) <= sla.sla_hours        THEN 1
    WHEN COALESCE(t.freshness_hours, 9999) <= sla.sla_hours * 1.2  THEN 2
    ELSE 3
  END AS sla_status_order,

  -- V3 3o: anomaly_source — distinguishes pipeline anomalies from business anomalies
  -- pipeline: technical issues (freshness breach, missing data, SLA violation)
  -- business: genuine network signal (volume deviation detected by z-score in monitoring_dag)
  -- This separation prevents mixing infrastructure alerts with operational insights.
  -- z-score anomalies are logged separately in dag_metrics — see mart_anomaly_history.
  CASE
    WHEN COALESCE(t.row_count, 0) = 0
      THEN 'pipeline'      -- missing data = pipeline issue
    WHEN COALESCE(t.freshness_hours, 9999) > sla.sla_hours * 1.2
      THEN 'pipeline'      -- critical freshness breach = pipeline issue
    WHEN COALESCE(t.freshness_hours, 9999) > sla.sla_hours
      THEN 'pipeline'      -- SLA breach = pipeline issue
    ELSE 'ok'              -- within SLA = no anomaly (business anomalies tracked in mart_anomaly_history)
  END AS anomaly_source

FROM sla_config sla
LEFT JOIN table_stats t ON sla.table_name = t.table_name
