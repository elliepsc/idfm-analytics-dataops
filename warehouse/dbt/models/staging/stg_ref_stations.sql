{{
  config(
    materialized='view',
    description='Station reference staging — id_ref_zdc (=ida in validations) + coordinates'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_ref_stations') }}
),

deduped AS (
  SELECT
    CAST(id_ref_zdc AS INT64)      AS station_id,
    TRIM(station_name)             AS station_name,
    CAST(latitude AS FLOAT64)      AS latitude,
    CAST(longitude AS FLOAT64)     AS longitude,
    TRIM(mode)                     AS transport_mode,
    TRIM(operator)                 AS operator,
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
    ROW_NUMBER() OVER (
      PARTITION BY id_ref_zdc
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM source
  WHERE id_ref_zdc IS NOT NULL
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND latitude BETWEEN 48.0 AND 49.5
    AND longitude BETWEEN 1.5 AND 3.5
)

SELECT
  station_id,
  station_name,
  latitude,
  longitude,
  transport_mode,
  operator,
  ingestion_ts
FROM deduped
WHERE rn = 1
