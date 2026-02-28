-- Staging - Ponctualité Transilien - mensuelle
{{
  config(
    materialized='view',
    description='Ponctualité Transilien - nettoyée et normalisée'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_punctuality') }}
),

cleaned AS (
  SELECT
    -- FIX V2: field 'month' is YYYY-MM format (STRING) — use PARSE_DATE('%Y-%m', month)
    -- V1 used PARSE_DATE('%Y-%m-%d', month) which fails because there is no day component.
    PARSE_DATE('%Y-%m', month) AS month_date,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m', month)) AS year,
    EXTRACT(MONTH FROM PARSE_DATE('%Y-%m', month)) AS month_num,

    -- Identifiers
    UPPER(TRIM(line_id)) AS line_id,
    TRIM(line_name) AS line_name,

    -- FIX V2: removed trains_planned and trains_departed — these fields do NOT exist
    -- in the ponctualite-mensuelle-transilien dataset (confirmed via API exploration 2026-02-27).
    -- Only taux_de_ponctualite is available.
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
