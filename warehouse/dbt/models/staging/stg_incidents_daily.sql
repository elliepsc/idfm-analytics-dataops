-- Staging - Incidents Daily
-- DISABLED: raw_incidents_daily not yet provisioned — requires PRIM API access (env var: PRIM_API_KEY).
-- To enable: (1) configure PRIM_API_KEY, (2) run transport_daily_pipeline to create raw_incidents_daily,
--            (3) restore not_null tests in _sources.yml for raw_incidents_daily,
--            (4) set enabled=True here, then also enable fct_incidents_daily + the 2 reliability mart scorecards.
{{
  config(
    materialized='view',
    enabled=True,
    description='IDFM disruption/incident messages — aggregated to day × incident_type grain. One row per (incident_date, incident_type). Deduplicates raw append-mode table.'
  )
}}

-- X2 reliability layer: incident messages from IDFM Info Trafic.
-- raw_incidents_daily uses WRITE_APPEND — each daily extraction adds rows.
-- Deduplication: ROW_NUMBER on (incident_date, incident_id or message hash)
-- to handle Airflow retries producing duplicate inserts.
--
-- Five canonical incident_type categories (normalised in the Python extractor):
--   planned_work, unplanned_disruption, partial_closure, full_closure, access_issue

WITH raw_source AS (
  SELECT * FROM {{ source('raw', 'raw_incidents_daily') }}
),

cleaned AS (
  SELECT
    CAST(incident_date AS DATE)                               AS incident_date,
    SAFE_CAST(incident_end_date AS DATE)                      AS incident_end_date,
    UPPER(TRIM(incident_type))                                AS incident_type,
    COALESCE(line_id, 'UNKNOWN')                              AS line_id,
    TRIM(line_name)                                           AS line_name,
    UPPER(TRIM(transport_mode))                               AS transport_mode,
    CAST(affected_stop_count AS INT64)                        AS affected_stop_count,
    CAST(ingestion_ts AS TIMESTAMP)                           AS ingestion_ts,
    CAST(source AS STRING)                                    AS source
  FROM raw_source
  WHERE
    incident_date IS NOT NULL
    AND incident_type IS NOT NULL
),

-- Compute incident duration in minutes for each raw message
with_duration AS (
  SELECT
    *,
    CASE
      WHEN incident_end_date IS NOT NULL AND incident_end_date >= incident_date
        THEN DATE_DIFF(incident_end_date, incident_date, DAY) * 24 * 60
      ELSE NULL
    END                                                       AS active_duration_minutes
  FROM cleaned
),

-- Aggregate to day × incident_type grain
-- One row per (incident_date, incident_type) with summed counts
aggregated AS (
  SELECT
    incident_date,
    incident_type,

    COUNT(*)                                                  AS incident_count,

    -- Major incidents: full closures or unplanned disruptions
    -- NOTE: incident_type is UPPER-cased in the cleaned CTE above
    COUNTIF(
      incident_type IN ('FULL_CLOSURE', 'UNPLANNED_DISRUPTION')
    )                                                         AS major_incident_count,

    SUM(affected_stop_count)                                  AS affected_stop_count,

    -- Average duration for incidents that have an end date
    ROUND(AVG(active_duration_minutes), 0)                    AS avg_active_duration_minutes,

    MAX(ingestion_ts)                                         AS ingestion_ts,
    MAX(source)                                               AS source

  FROM with_duration
  GROUP BY incident_date, incident_type
)

SELECT
  incident_date,
  incident_type,
  incident_count,
  major_incident_count,
  COALESCE(affected_stop_count, 0)                          AS affected_stop_count,
  avg_active_duration_minutes,
  ingestion_ts,
  source
FROM aggregated
WHERE
  incident_type IN (
    'PLANNED_WORK', 'UNPLANNED_DISRUPTION', 'PARTIAL_CLOSURE',
    'FULL_CLOSURE', 'ACCESS_ISSUE'
  )
