--- Stopping point reference staging model - latest version

{{
  config(
    materialized='view',
    description='Stop reference staging model - latest version (1 record per stop_id, with data quality checks and transformations applied)'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_ref_stops') }}
),

latest AS (
  SELECT
    -- Identifiers
    UPPER(TRIM(stop_id)) AS stop_id,
    TRIM(stop_name) AS stop_name,

    -- Geolocalisation
    CAST(latitude AS FLOAT64) AS latitude,
    CAST(longitude AS FLOAT64) AS longitude,

    -- Location
    TRIM(town) AS town,

    -- Metadata
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,

    -- Row number for deduplication
    ROW_NUMBER() OVER (
      PARTITION BY UPPER(TRIM(stop_id))
      ORDER BY CAST(ingestion_ts AS TIMESTAMP) DESC
    ) AS rn

  FROM source

  WHERE
    stop_id IS NOT NULL
    AND stop_name IS NOT NULL
    -- Valid geolocalisation for Île-de-France (approximate bounding box)
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
WHERE rn = 1  -- Keep only the latest record for each stop_id based on ingestion timestamp
