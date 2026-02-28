-- Mart - Unified metrics view across all monitored tables
{{
  config(
    materialized='table',
    description='Unified metrics aggregating health data from all monitored tables'
  )
}}

-- FIX V2: V1 file was truncated — started mid-CTE with no WITH or config block.
-- Reconstructed as a simple union over the three metrics models.

WITH validations_metrics AS (
  SELECT * FROM {{ ref('metrics_fct_validations_daily') }}
),

punctuality_metrics AS (
  SELECT * FROM {{ ref('metrics_fct_punctuality_monthly') }}
),

scorecard_metrics AS (
  SELECT * FROM {{ ref('metrics_mart_network_scorecard_monthly') }}
),

all_metrics AS (
  SELECT * FROM validations_metrics
  UNION ALL
  SELECT * FROM punctuality_metrics
  UNION ALL
  SELECT * FROM scorecard_metrics
),

sla_config AS (
  SELECT 'fct_validations_daily'          AS table_name, 30       AS sla_hours UNION ALL
  SELECT 'fct_punctuality_monthly',        24 * 45 UNION ALL
  SELECT 'mart_network_scorecard_monthly', 24 * 45
),

final AS (
  SELECT
    m.table_name,
    m.metric_date,
    m.row_count,
    m.freshness_hours,
    m.null_percentage,
    m.duplicate_count,
    s.sla_hours,
    (m.freshness_hours <= s.sla_hours AND m.row_count > 0) AS sla_met
  FROM all_metrics m
  LEFT JOIN sla_config s ON m.table_name = s.table_name
)

SELECT * FROM final
