--  Staging - Validations réseau ferré
{{
  config(
    materialized='view',
    description='Validations réseau ferré - nettoyées et normalisées'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_validations') }}
),

cleaned AS (
  SELECT
    -- Dates
    PARSE_DATE('%Y-%m-%d', date) AS validation_date,
    DATE_TRUNC(PARSE_DATE('%Y-%m-%d', date), MONTH) AS validation_month,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS year,
    EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', date)) AS month,
    EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y-%m-%d', date)) AS day_of_week,

    -- Identifiants
    UPPER(TRIM(stop_id)) AS stop_id,
    TRIM(stop_name) AS stop_name,
    UPPER(TRIM(line_id)) AS line_id,
    TRIM(line_name) AS line_name,

    -- Catégorie titre
    UPPER(TRIM(ticket_type)) AS ticket_type,

    -- Métriques
    CAST(validation_count AS INT64) AS validation_count,

    -- Metadata
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
    source

  FROM source

  WHERE
    -- Filtre qualité
    date IS NOT NULL
    AND stop_id IS NOT NULL
    AND validation_count IS NOT NULL
    AND CAST(validation_count AS INT64) >= 0  -- Pas de compteurs négatifs
)

SELECT * FROM cleaned
