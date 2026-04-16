-- X3 mart: enabled. fct_incidents_daily runs with 0 rows until PRIM API is configured (LEFT JOIN — no crash).
-- fct_service_quality_quarterly is populated from transport_quarterly_pipeline.
{{
  config(
    materialized='table',
    enabled=True,
    description='Monthly reliability scorecard per line. Grain: 1 line × 1 month. Combines trusted validations (demand), service quality (quarterly, downsampled to month), and incidents. Covers METRO + RER + TRAIN.'
  )
}}

-- X3 reliability layer: unified scorecard replacing punctuality-only risk model.
--
-- Sources:
--   demand   → mart_validations_line_monthly (METRO + RER trusted mappings)
--   quality  → fct_service_quality_quarterly (RATP Métro + RER + SNCF, quarterly grain)
--   incidents → fct_incidents_daily (all modes, aggregated to month)
--
-- Service quality is quarterly — each month within a quarter inherits the same
-- indicator values. We use MAX(indicator_value) per (line_id, month) after
-- the month→quarter join to get one row per line × month.
--
-- reliability_score = weighted composite:
--   40% demand trend (MoM growth — proxy for service attractiveness)
--   40% avg service quality vs target
--   20% incident pressure (inverse of major_incident_count, capped)
--
-- reliability_band:
--   high    → score >= 70
--   medium  → score >= 40
--   low     → score <  40

WITH demand AS (
  SELECT
    validation_month                              AS month_date,
    line_id,
    line_name,
    transport_mode,
    operator,
    validation_count                              AS total_validations,
    days_with_data
  FROM {{ ref('mart_validations_line_monthly') }}
),

-- MoM growth from previous month (same approach as mart_network_scorecard_monthly)
demand_with_growth AS (
  SELECT
    *,
    LAG(total_validations) OVER (
      PARTITION BY line_id
      ORDER BY month_date
    )                                             AS prev_month_validations,

    ROUND(
      SAFE_DIVIDE(
        total_validations - LAG(total_validations) OVER (PARTITION BY line_id ORDER BY month_date),
        NULLIF(LAG(total_validations) OVER (PARTITION BY line_id ORDER BY month_date), 0)
      ) * 100,
      2
    )                                             AS mom_growth_pct
  FROM demand
),

-- Service quality: expand quarterly values to monthly grain
-- Each month gets the quarter it belongs to via DATE_TRUNC(month, QUARTER)
service_quality_monthly AS (
  SELECT
    DATE_TRUNC(
      DATE_ADD(quarter_date, INTERVAL (m - 1) MONTH),
      MONTH
    )                                             AS month_date,
    line_id,
    -- Average across all indicators for that line × quarter
    ROUND(AVG(indicator_value), 2)                AS avg_service_quality,
    ROUND(AVG(target_value), 2)                   AS avg_quality_target,
    ROUND(AVG(gap_to_target), 2)                  AS avg_gap_to_target,
    -- % of indicators meeting or exceeding target
    ROUND(
      COUNTIF(quality_band IN ('meets_target', 'exceeds_target')) * 100.0
      / NULLIF(COUNT(*), 0),
      1
    )                                             AS pct_indicators_on_target,
    transport_mode,
    operator
  FROM {{ ref('fct_service_quality_quarterly') }}
  -- Expand each quarter into its 3 months (month offset 0, 1, 2)
  CROSS JOIN UNNEST([1, 2, 3]) AS m
  GROUP BY
    DATE_TRUNC(DATE_ADD(quarter_date, INTERVAL (m - 1) MONTH), MONTH),
    line_id,
    transport_mode,
    operator
),

-- Incidents: aggregate to month level (all incident types combined per month)
incidents_monthly AS (
  SELECT
    incident_month                                AS month_date,
    SUM(incident_count)                           AS incident_count,
    SUM(major_incident_count)                     AS major_incident_count,
    SUM(affected_stop_count)                      AS affected_stop_count
  FROM {{ ref('fct_incidents_daily') }}
  GROUP BY incident_month
),

-- Join all three sources
-- Demand is the driving spine — lines without validation data are excluded.
-- Service quality and incidents are LEFT JOINed (may be NULL for some months).
combined AS (
  SELECT
    d.month_date,
    d.line_id,
    COALESCE(d.line_name, sq.line_id)             AS line_name,
    COALESCE(d.transport_mode, sq.transport_mode) AS transport_mode,
    COALESCE(d.operator, sq.operator)             AS operator,

    -- Demand
    d.total_validations,
    d.days_with_data,
    d.prev_month_validations,
    d.mom_growth_pct,

    -- Service quality (quarterly, downsampled)
    sq.avg_service_quality,
    sq.avg_quality_target,
    sq.avg_gap_to_target,
    sq.pct_indicators_on_target,

    -- Incidents (network-wide, not per-line — joined on month only)
    -- NOTE: fct_incidents_daily is not yet line-level (X2 v1 grain = day × type).
    -- When line-level incident data is available, replace this with a per-line join.
    i.incident_count,
    i.major_incident_count,
    i.affected_stop_count

  FROM demand_with_growth d
  LEFT JOIN service_quality_monthly sq
    ON d.line_id = sq.line_id AND d.month_date = sq.month_date
  LEFT JOIN incidents_monthly i
    ON d.month_date = i.month_date
),

-- Compute composite reliability score (0–100)
scored AS (
  SELECT
    *,

    -- Component 1: demand trend score (40 pts)
    -- Positive MoM growth → high score; flat → 20; declining → lower
    ROUND(
      LEAST(40,
        GREATEST(0,
          CASE
            WHEN mom_growth_pct IS NULL   THEN 20.0
            WHEN mom_growth_pct >= 5.0    THEN 40.0
            WHEN mom_growth_pct >= 0.0    THEN 20.0 + mom_growth_pct * 4.0
            WHEN mom_growth_pct >= -10.0  THEN GREATEST(0, 20.0 + mom_growth_pct * 2.0)
            ELSE 0.0
          END
        )
      ),
      1
    )                                             AS demand_score,

    -- Component 2: service quality score (40 pts)
    -- Based on % of indicators on target; NULL → 20 (neutral, data gap)
    ROUND(
      LEAST(40,
        GREATEST(0,
          COALESCE(pct_indicators_on_target * 0.4, 20.0)
        )
      ),
      1
    )                                             AS quality_score,

    -- Component 3: incident pressure score (20 pts)
    -- Fewer major incidents → higher score; capped at 5 major incidents = 0 pts
    ROUND(
      GREATEST(0,
        20.0 - COALESCE(major_incident_count, 0) * 4.0
      ),
      1
    )                                             AS incident_score

  FROM combined
)

SELECT
  month_date,
  line_id,
  line_name,
  transport_mode,
  operator,

  -- Demand
  total_validations,
  days_with_data,
  prev_month_validations,
  mom_growth_pct,

  -- Service quality
  avg_service_quality,
  avg_quality_target,
  avg_gap_to_target,
  pct_indicators_on_target,

  -- Incidents
  COALESCE(incident_count, 0)                   AS incident_count,
  COALESCE(major_incident_count, 0)             AS major_incident_count,
  COALESCE(affected_stop_count, 0)              AS affected_stop_count,

  -- Score components
  demand_score,
  quality_score,
  incident_score,
  ROUND(demand_score + quality_score + incident_score, 1) AS reliability_score,

  -- Reliability band
  CASE
    WHEN (demand_score + quality_score + incident_score) >= 70 THEN 'high'
    WHEN (demand_score + quality_score + incident_score) >= 40 THEN 'medium'
    ELSE 'low'
  END                                           AS reliability_band,

  CURRENT_TIMESTAMP()                           AS dbt_updated_at

FROM scored
ORDER BY month_date DESC, reliability_score DESC
