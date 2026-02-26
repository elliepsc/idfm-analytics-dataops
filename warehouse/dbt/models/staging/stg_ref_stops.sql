--- Référentiel des arrêts - version latest

{{
  config(
    materialized='view',
    description='Référentiel des arrêts - version latest'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_ref_stops') }}
),

latest AS (
  SELECT
    -- Identifiants
    UPPER(TRIM(stop_id)) AS stop_id,
    TRIM(stop_name) AS stop_name,

    -- Géolocalisation
    CAST(latitude AS FLOAT64) AS latitude,
    CAST(longitude AS FLOAT64) AS longitude,

    -- Localisation
    TRIM(town) AS town,

    -- Metadata
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,

    -- Row number pour déduplication
    ROW_NUMBER() OVER (
      PARTITION BY UPPER(TRIM(stop_id))
      ORDER BY CAST(ingestion_ts AS TIMESTAMP) DESC
    ) AS rn

  FROM source

  WHERE
    stop_id IS NOT NULL
    AND stop_name IS NOT NULL
    -- Coordonnées valides (approximativement Île-de-France)
    AND CAST(latitude AS FLOAT64) BETWEEN 48.0 AND 49.5
    AND CAST(longitude AS FLOAT64) BETWEEN 1.5 AND 3.5
)

SELECT
  stop_id,
  stop_name,
  latitude,
  longitude,
  town,
  ingestion_ts
FROM latest
WHERE rn = 1  -- Garde la version la plus récente
