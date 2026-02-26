punctuality_metrics AS (
  SELECT
    'fct_punctuality_monthly' AS table_name,
    CURRENT_DATE() AS metric_date,
    COUNT(*) AS row_count,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingestion_ts), HOUR) AS freshness_hours,
    ROUND(100.0 * AVG(CASE WHEN line_id IS NULL THEN 1.0 ELSE 0.0 END), 2) AS null_percentage,
    SUM(CASE WHEN dup_cnt > 1 THEN dup_cnt - 1 ELSE 0 END) AS duplicate_count
  FROM (
    SELECT
      *,
      COUNT(*) OVER (
        PARTITION BY line_id, month_date
      ) AS dup_cnt
    FROM {{ ref('fct_punctuality_monthly') }}
    WHERE month_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
  )
),
