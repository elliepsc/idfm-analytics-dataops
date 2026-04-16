-- Incidents fact table at the grain of 1 day × 1 incident_type
-- NOTE: raw_incidents_daily exists but is empty until PRIM API is configured (PRIM_API_KEY env var).
-- The ODS fallback (cartes-des-travaux) populates planned works only when no PRIM key is set.
{{
  config(
    materialized='incremental',
    enabled=True,
    unique_key='incident_key',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "incident_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['incident_type'],
    description='Daily incidents fact (grain = 1 day × 1 incident_type). Source: IDFM Info Trafic messages. Insert-overwrite on day partition — safe because the extractor fetches full day on each run.'
  )
}}

-- X2 reliability layer: daily incident KPIs by type.
-- insert_overwrite strategy is correct here: the extractor always fetches
-- complete messages for a given day, so replacing the partition is safe.
-- Unlike punctuality (SNCF retroactive corrections), IDFM incident history
-- is final once published.
--
-- To backfill a date range:
--   1. Re-run extract_incidents_daily.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
--   2. Re-run load_bigquery_raw.py for the incidents table
--   3. dbt run --select fct_incidents_daily

WITH incidents AS (
  SELECT * FROM {{ ref('stg_incidents_daily') }}

  {% if is_incremental() %}
    {% if target.name == 'dev' %}
  -- Dev: limit scan to configured date window
  WHERE incident_date BETWEEN '{{ var("dev_start_date") }}' AND '{{ var("dev_end_date") }}'
    {% else %}
  -- Prod: reprocess last 7 days (Airflow retries may re-ingest recent messages)
  WHERE incident_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    {% endif %}
  {% endif %}
)

SELECT
  -- Surrogate key — unique per (incident_date, incident_type)
  {{ dbt_utils.generate_surrogate_key(['incident_date', 'incident_type']) }} AS incident_key,

  -- Date grain
  incident_date,
  EXTRACT(YEAR FROM incident_date)      AS year,
  EXTRACT(MONTH FROM incident_date)     AS month_num,
  EXTRACT(QUARTER FROM incident_date)   AS quarter_num,
  DATE_TRUNC(incident_date, MONTH)      AS incident_month,
  DATE_TRUNC(incident_date, ISOWEEK)    AS incident_week,

  -- Incident category
  -- NOTE: values are stored UPPER in staging to match the 5-category taxonomy:
  -- PLANNED_WORK | UNPLANNED_DISRUPTION | PARTIAL_CLOSURE | FULL_CLOSURE | ACCESS_ISSUE
  incident_type,

  -- Metrics
  incident_count,
  major_incident_count,
  affected_stop_count,
  avg_active_duration_minutes,

  -- Metadata
  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM incidents
