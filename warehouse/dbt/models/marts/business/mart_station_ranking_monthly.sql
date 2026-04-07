{{
  config(
    materialized='table',
    description='Monthly station ranking by validation volume with rank change. Grain: 1 station x 1 month. A station dropping from rank 5 to 12 signals an unresolved incident or service change.'
  )
}}

-- V3 3f: mart_station_ranking_monthly
-- Business question: Which stations are rising or falling in the network ranking?
--   Raw volume doesn't reveal rank shifts. A station losing 5 rank positions
--   in one month may signal an incident, line closure, or structural change.
--
-- Source: mart_validations_station_daily (pre-joined with coordinates)
-- Pattern: Kenya — RANK() OVER (PARTITION BY year), Metal — agg_top_songs
-- NOTE: BigQuery does not allow nesting analytic functions (LAG(RANK() OVER ...))
--       We compute rank in CTE 1, then LAG in CTE 2.

WITH monthly_station AS (
  SELECT
    DATE_TRUNC(validation_date, MONTH)  AS validation_month,
    station_id_zdc,
    stop_name,
    station_name,
    transport_mode,
    operator,
    SUM(daily_validation_count)         AS monthly_validations
  FROM {{ ref('mart_validations_station_daily') }}
  GROUP BY 1, 2, 3, 4, 5, 6
),

-- Step 1: compute rank per month
with_rank AS (
  SELECT
    validation_month,
    station_id_zdc,
    stop_name,
    station_name,
    transport_mode,
    operator,
    monthly_validations,
    RANK() OVER (
      PARTITION BY validation_month
      ORDER BY monthly_validations DESC
    ) AS station_rank
  FROM monthly_station
),

-- Step 2: LAG on the already-computed rank (BigQuery forbids nesting)
with_prev_rank AS (
  SELECT
    *,
    LAG(station_rank, 1) OVER (
      PARTITION BY station_id_zdc
      ORDER BY validation_month
    ) AS prev_month_rank
  FROM with_rank
)

SELECT
  validation_month,
  station_id_zdc,
  stop_name,
  station_name,
  transport_mode,
  operator,
  monthly_validations,
  station_rank,
  prev_month_rank,

  -- Rank change (negative = improved = moved up)
  station_rank - prev_month_rank AS rank_change,

  -- Trend label
  CASE
    WHEN prev_month_rank IS NULL               THEN 'new_entry'
    WHEN (station_rank - prev_month_rank) < -5 THEN 'rising'
    WHEN (station_rank - prev_month_rank) > 5  THEN 'falling'
    ELSE                                            'stable'
  END AS rank_trend,

  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM with_prev_rank
ORDER BY validation_month, station_rank
