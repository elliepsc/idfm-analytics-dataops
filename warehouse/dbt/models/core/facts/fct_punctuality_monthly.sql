-- Punctuality fact table at the grain of 1 line × 1 month
{{
  config(
    materialized='incremental',
    unique_key='punctuality_key',
    incremental_strategy='merge',
    partition_by={
      "field": "month_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=['line_id'],
    description='Punctuality fact table (grain = 1 line × 1 month). Deduplicated at staging level — unique_key guaranteed.'
  )
}}

-- P3-A: switched from insert_overwrite to merge strategy.
-- Rationale: insert_overwrite replaces the full partition on each run, which means
-- SNCF duplicates from raw_punctuality were reloaded every time.
-- With merge on punctuality_key (hash of line_id + month_date):
--   - new months are inserted
--   - corrected months from SNCF retroactive updates are updated in place
--   - duplicates never reach this table (deduplicated upstream in stg_punctuality_monthly)
-- Result: unique_fct_punctuality_monthly_punctuality_key WARN disappears.

WITH punctuality AS (
  SELECT * FROM {{ ref('stg_punctuality_monthly') }}

  {% if is_incremental() %}
    {% if target.name == 'dev' %}
  -- Dev: limit scan to configured date window
  WHERE month_date BETWEEN '{{ var("dev_start_date") }}' AND '{{ var("dev_end_date") }}'
    {% else %}
  -- Prod: process last 3 months to catch SNCF retroactive corrections
  -- (SNCF regularly corrects data up to 60 days after the reference month)
  WHERE month_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
    {% endif %}
  {% endif %}
)

SELECT
  -- Surrogate key — unique per (line_id, month_date) after staging deduplication
  {{ dbt_utils.generate_surrogate_key(['line_id', 'month_date']) }} AS punctuality_key,

  -- Date
  month_date,
  year,
  -- FIX V2: renamed 'month' → 'month_num' in stg_punctuality_monthly
  -- to avoid conflict with the reserved SQL keyword 'month'.
  month_num,

  -- Dimension FK
  line_id,

  -- Metrics
  punctuality_rate,
  quality_category,

  -- Metadata
  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM punctuality
