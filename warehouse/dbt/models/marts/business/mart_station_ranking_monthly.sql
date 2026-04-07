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

ranked AS (
  SELECT
    validation_month,
    station_id_zdc,
    stop_name,
    station_name,
    transport_mode,
    operator,
    monthly_validations,

    -- Network rank this month
    RANK() OVER (
      PARTITION BY validation_month
      ORDER BY monthly_validations DESC
    ) AS station_rank,

    -- Previous month rank for comparison
    LAG(
      RANK() OVER (
        PARTITION BY validation_month
        ORDER BY monthly_validations DESC
      ), 1
    ) OVER (
      PARTITION BY station_id_zdc
      ORDER BY validation_month
    ) AS prev_month_rank

  FROM monthly_station
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
    WHEN prev_month_rank IS NULL              THEN 'new_entry'
    WHEN (station_rank - prev_month_rank) < -5 THEN 'rising'
    WHEN (station_rank - prev_month_rank) > 5  THEN 'falling'
    ELSE                                           'stable'
  END AS rank_trend,

  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM ranked
ORDER BY validation_month, station_rank
