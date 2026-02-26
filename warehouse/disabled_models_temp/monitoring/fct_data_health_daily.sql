-- Mart - Data Health & SLA monitoring

{{
  config(
    materialized='table',
    description='Monitoring quotidien de la santé des données (freshness, volume, SLA)'
  )
}}

WITH sla_config AS (
  -- Configuration des SLA par table
  SELECT 'fct_validations_daily' AS table_name, 30 AS sla_hours UNION ALL
  SELECT 'fct_punctuality_monthly', 24 * 45 UNION ALL
  SELECT 'mart_network_scorecard_monthly', 24 * 45
),
