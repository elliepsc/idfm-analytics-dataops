-- Service quality fact table at the grain of 1 line x 1 quarter x 1 indicator
{{
  config(
    materialized='table',
    enabled=True,
    cluster_by=['line_id', 'indicator_label'],
    description='Service quality fact (grain = 1 line x 1 quarter x 1 indicator). Covers RATP Metro + RER + SNCF Transilien. Full rebuild each quarterly run from the latest cleaned snapshot.'
  )
}}

-- X1 reliability layer: replaces punctuality-only quality KPI.
-- The upstream dataset is a compact full snapshot, so a full rebuild is safer
-- than incremental merge: rows filtered out in staging must disappear from the
-- final fact on the next quarterly run.

WITH service_quality AS (
  SELECT * FROM {{ ref('stg_service_quality_quarterly') }}
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['line_id', 'quarter_date', 'indicator_label']) }} AS service_quality_key,

  quarter_date,
  quarter_num,
  year,
  quarter,

  line_id,
  line_name,

  indicator_label,
  indicator_theme,
  indicator_value,
  target_value,
  gap_to_target,
  quality_band,

  transport_mode,
  operator,

  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM service_quality
