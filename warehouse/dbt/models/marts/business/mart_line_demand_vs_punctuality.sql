{{
  config(
    materialized='table',
    description='Monthly demand vs punctuality cross by line — pivot analytique central. Grain: 1 line x 1 month. Corrects mart_network_scorecard_monthly which joins validations only at network level.'
  )
}}

-- V3 3q: mart_line_demand_vs_punctuality — PIVOT ANALYTIQUE CENTRAL
-- Business questions (3 explicit):
--   1. Do high-demand lines degrade faster than low-demand ones?
--   2. Which lines remain resilient under ridership pressure?
--   3. Are some lines structurally fragile even without high demand?
--
-- Why this mart matters:
--   mart_network_scorecard_monthly cannot answer these questions because
--   raw_validations has no line_id — it only has stop_id.
--   The bridge: fct_validations_daily.stop_id
--             → stg_ref_stop_lines.line_id
--             → fct_punctuality_monthly.line_id
--
-- ⚠️ CONDITIONAL — validate stop_lines coverage before using in production:
--   SELECT COUNT(*), COUNT(DISTINCT line_id) FROM {{ ref('stg_ref_stop_lines') }}
--   If line_id coverage is low (< 80% of punctuality lines covered),
--   demand_index will be unreliable for low-coverage lines.
--
-- Source: fct_validations_daily + stg_ref_stop_lines + fct_punctuality_monthly
-- Pattern: interpretive analytics — 3 explicit business hypotheses

WITH validations_by_line AS (
  -- Aggregate validations to line level via line_code_res → line_code_mapping seed
  -- Bridge: fct_validations_daily.line_code_res → line_code_mapping.line_code_res
  --       → line_code_mapping.line_shortname → fct_punctuality_monthly.line_id
  -- Coverage: 10 RER/Transilien lines (SNCF open data only, no RATP metro)
  -- line_code_trns 800/810 = network codes; line_code_res = individual line codes
  SELECT
    DATE_TRUNC(f.validation_date, MONTH) AS validation_month,
    m.line_shortname                      AS line_id,
    SUM(f.validation_count)               AS line_validations
  FROM {{ ref('fct_validations_daily') }} f
  INNER JOIN {{ ref('line_code_mapping') }} m
    ON f.line_code_res = m.line_code_res
  WHERE f.line_code_res != '<NA>'
  GROUP BY 1, 2
),

-- Network monthly total for normalisation
network_monthly AS (
  SELECT
    validation_month,
    SUM(line_validations) AS network_total
  FROM validations_by_line
  GROUP BY 1
),

-- Normalised demand index (line share of total network validations)
demand_indexed AS (
  SELECT
    v.validation_month,
    v.line_id,
    v.line_validations,
    n.network_total,
    ROUND(
      {{ safe_divide('v.line_validations', 'n.network_total') }} * 100,
      2
    ) AS demand_index_pct,

    -- Demand tier (relative to network)
    NTILE(3) OVER (
      PARTITION BY v.validation_month
      ORDER BY v.line_validations
    ) AS demand_tercile  -- 1=low, 2=medium, 3=high

  FROM validations_by_line v
  INNER JOIN network_monthly n ON v.validation_month = n.validation_month
),

-- Join with punctuality
combined AS (
  SELECT
    d.validation_month,
    d.line_id,
    l.line_name,
    l.transport_mode,
    l.operator,
    d.line_validations,
    d.demand_index_pct,
    d.demand_tercile,
    p.punctuality_rate,
    p.quality_category,
    t.target_punctuality_rate,

    -- Gap to punctuality target (if available)
    ROUND(p.punctuality_rate - t.target_punctuality_rate, 2) AS gap_to_target

  FROM demand_indexed d
  LEFT JOIN {{ ref('fct_punctuality_monthly') }} p
    ON d.line_id = p.line_id AND d.validation_month = p.month_date
  LEFT JOIN {{ ref('dim_line') }} l
    ON d.line_id = l.line_id
  LEFT JOIN {{ ref('line_punctuality_target') }} t
    ON UPPER(TRIM(d.line_id)) = UPPER(TRIM(t.line_id))
)

SELECT
  validation_month,
  line_id,
  line_name,
  transport_mode,
  operator,
  line_validations,
  demand_index_pct,
  demand_tercile,
  punctuality_rate,
  quality_category,
  target_punctuality_rate,
  gap_to_target,

  -- Core interpretive output: demand × punctuality profile
  -- Answers the 3 business questions explicitly
  CASE
    WHEN punctuality_rate IS NULL
      THEN 'no_punctuality_data'

    -- Q2: Resilient under pressure — high demand, maintains punctuality
    WHEN demand_tercile = 3 AND punctuality_rate >= 90.0
      THEN 'resilient_under_pressure'

    -- Q1: Fragile under pressure — high demand, punctuality degrades
    WHEN demand_tercile = 3 AND punctuality_rate < 90.0
      THEN 'fragile_under_pressure'

    -- Q3: Structurally fragile — low demand but still underperforming
    WHEN demand_tercile = 1 AND punctuality_rate < 88.0
      THEN 'structurally_fragile'

    -- Low demand, good punctuality
    WHEN demand_tercile = 1 AND punctuality_rate >= 90.0
      THEN 'stable_low_demand'

    -- Medium demand
    ELSE 'moderate'
  END AS demand_punctuality_profile,

  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM combined
WHERE punctuality_rate IS NOT NULL  -- only lines with punctuality data
ORDER BY validation_month, demand_index_pct DESC
