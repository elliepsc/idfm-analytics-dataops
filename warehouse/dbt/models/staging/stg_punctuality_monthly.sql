-- Staging - Punctuality Monthly
{{
  config(
    materialized='view',
    description='Punctuality performance data at monthly grain, deduplicated to 1 row per (line_id, month_date). Grain: 1 line_id x 1 month.'
  )
}}

-- P3-A: SNCF source occasionally sends multiple rows for the same (line_id, month_date)
-- (e.g. retroactive corrections, duplicate API responses).
-- This caused 156 WARN unique_fct_punctuality_monthly_punctuality_key at test time.
-- Fix: ROW_NUMBER() deduplication here in staging keeps the most recently ingested row.
-- Combined with merge strategy in fct_punctuality_monthly, this eliminates duplicates
-- both in new loads and when SNCF sends retroactive corrections for past months.

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_punctuality') }}
),

deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY UPPER(TRIM(line_id)), PARSE_DATE('%Y-%m', MONTH)
      ORDER BY CAST(ingestion_ts AS TIMESTAMP) DESC
    ) AS rn
  FROM source
  WHERE
    month IS NOT NULL
    AND line_id IS NOT NULL
    AND punctuality_rate IS NOT NULL
    AND CAST(punctuality_rate AS FLOAT64) BETWEEN 0 AND 100
)

SELECT
  PARSE_DATE('%Y-%m', MONTH)                        AS month_date,
  EXTRACT(YEAR FROM PARSE_DATE('%Y-%m', MONTH))     AS year,
  EXTRACT(MONTH FROM PARSE_DATE('%Y-%m', MONTH))    AS month_num,
  UPPER(TRIM(line_id))                              AS line_id,
  TRIM(line_name)                                   AS line_name,
  CAST(punctuality_rate AS FLOAT64)                 AS punctuality_rate,
  CASE
    WHEN CAST(punctuality_rate AS FLOAT64) >= 95.0 THEN 'excellent'
    WHEN CAST(punctuality_rate AS FLOAT64) >= 90.0 THEN 'good'
    WHEN CAST(punctuality_rate AS FLOAT64) >= 80.0 THEN 'acceptable'
    ELSE 'poor'
  END                                               AS quality_category,
  CAST(ingestion_ts AS TIMESTAMP)                   AS ingestion_ts,
  source
FROM deduplicated
WHERE rn = 1
