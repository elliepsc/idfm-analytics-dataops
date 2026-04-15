-- Stop reference staging model - latest version

{{
  config(
    materialized='view',
    description='Stop reference staging model - latest version (1 record per stop_id). Source: arrets-lignes export (denormalized: 1 stop x N lines). Deduplication keeps the most recently ingested row per stop_id.'
  )
}}

-- Note on transport_mode / operator: the arrets-lignes source is 1 stop × N lines.
-- After deduplication on stop_id, transport_mode and operator reflect the most recently
-- ingested stop×line record. For stops served by a single mode/operator (the vast majority)
-- this is exact. For multimodal interchange stops, it is a best-effort primary assignment.

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
    TRIM(CAST(insee_code AS STRING)) AS insee_code, -- INSEE code as string to preserve leading zeros (airflow)

    -- Transport classification (best-effort for multimodal stops — see note above)
    UPPER(TRIM(transport_mode)) AS transport_mode,
    TRIM(operator) AS operator,

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
  insee_code,
  transport_mode,
  operator,
  ingestion_ts
FROM latest
WHERE rn = 1  -- Keep only the latest record for each stop_id based on ingestion timestamp
