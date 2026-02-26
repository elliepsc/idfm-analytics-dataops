--- Staging - Ponctualité Transilien - mensuelle

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
    -- Date (premier jour du mois)
    PARSE_DATE('%Y-%m-%d', month) AS month_date,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', month)) AS year,
    EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', month)) AS month,

    -- Identifiants
    UPPER(TRIM(line_id)) AS line_id,
    TRIM(line_name) AS line_name,

    -- Métriques
    CAST(punctuality_rate AS FLOAT64) AS punctuality_rate,
    CAST(trains_planned AS INT64) AS trains_planned,
    CAST(trains_departed AS INT64) AS trains_departed,

    -- Indicateurs dérivés
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
