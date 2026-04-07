{{
  config(
    materialized='table',
    description='Historical z-score anomaly log from dag_metrics. Grain: 1 anomaly event. Brings dag_metrics into dbt lineage — enables meta-analysis of monitoring quality and threshold calibration.'
  )
}}

-- V3 3i: mart_anomaly_history
-- Business question: How many anomalies per month? Is the z-score threshold 2.5 well calibrated?
--   dag_metrics is written by the Airflow Python DAG and exists outside dbt lineage.
--   This mart pulls it into the analytical layer for meta-monitoring.
--
-- Meta-analytical value:
--   - Are anomalies concentrated on school holidays? (bias from z-score window)
--   - Are high_volume anomalies more frequent in summer? (seasonal pattern)
--   - Is z-score=2.5 generating too many / too few alerts?
--
-- Source: transport_raw.dag_metrics (written by monitoring_dag.py)
-- Pattern: Kenya — KPI synthétiques lisibles, IDFM z-score existant
-- Note: If dag_metrics doesn't exist yet in your environment, this model will
--       fail gracefully on dbt build — add to sources once confirmed in BQ.

WITH anomalies AS (
  SELECT
    DATE(ingestion_ts)                    AS anomaly_date,
    DATE_TRUNC(DATE(ingestion_ts), MONTH) AS anomaly_month,
    dag_id,
    task_id,
    run_id,
    z_score,
    is_anomaly,
    nb_records                            AS today_count,
    ingestion_ts

  FROM `{{ env_var('GCP_PROJECT_ID') }}.transport_raw.dag_metrics`
  WHERE is_anomaly = TRUE
    AND z_score IS NOT NULL
),

enriched AS (
  SELECT
    anomaly_date,
    anomaly_month,
    dag_id,
    task_id,
    today_count,
    z_score,

    -- Direction of anomaly
    CASE
      WHEN z_score < -2.5 THEN 'low_volume'
      WHEN z_score >  2.5 THEN 'high_volume'
      ELSE 'normal'
    END AS direction,

    -- Severity bucket
    CASE
      WHEN ABS(z_score) >= 5.0 THEN 'critical'
      WHEN ABS(z_score) >= 3.5 THEN 'high'
      ELSE 'moderate'
    END AS severity,

    -- Count of anomalies this month (for calibration analysis)
    COUNT(*) OVER (
      PARTITION BY DATE_TRUNC(DATE(ingestion_ts), MONTH)
    ) AS anomalies_this_month,

    ingestion_ts

  FROM anomalies
)

SELECT
  anomaly_date,
  anomaly_month,
  dag_id,
  task_id,
  today_count,
  z_score,
  direction,
  severity,
  anomalies_this_month,
  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM enriched
ORDER BY ingestion_ts DESC
