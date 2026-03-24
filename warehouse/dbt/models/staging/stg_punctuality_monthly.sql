-- Staging - Ponctuality Monthly
{{
  config(
    materialized='view',
    description='Punctuality performance data at monthly grain, with quality categorization (grain = 1 line_id x month)'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_punctuality') }}
),

cleaned AS (
  SELECT
    -- FIX V2: field 'month' is YYYY-MM format (STRING) — use PARSE_DATE('%Y-%m', month)
    -- V1 used PARSE_DATE('%Y-%m-%d', month) which fails because there is no day component.
    PARSE_DATE('%Y-%m', MONTH) AS month_date,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m', MONTH)) AS year,
    EXTRACT(MONTH FROM PARSE_DATE('%Y-%m', MONTH)) AS month_num,

    -- Identifiers
    UPPER(TRIM(line_id)) AS line_id,
    TRIM(line_name) AS line_name,

    -- FIX V2: removed trains_planned and trains_departed — these fields do NOT exist
    -- in the raw data and were erroneously included in V1. Only punctuality_rate is available.
    -- Only punctuality_rate is available.
    CAST(punctuality_rate AS FLOAT64) AS punctuality_rate,

    -- Derived quality indicator
    CASE
      WHEN CAST(punctuality_rate AS FLOAT64) >= 95.0 THEN 'excellent'
      WHEN CAST(punctuality_rate AS FLOAT64) >= 90.0 THEN 'good'
      WHEN CAST(punctuality_rate AS FLOAT64) >= 80.0 THEN 'acceptable'
      ELSE 'poor'
    END AS quality_category,

    -- Metadata
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
    source

  FROM source

  WHERE
    month IS NOT NULL
    AND line_id IS NOT NULL
    AND punctuality_rate IS NOT NULL
    AND CAST(punctuality_rate AS FLOAT64) BETWEEN 0 AND 100
)

SELECT * FROM cleaned
