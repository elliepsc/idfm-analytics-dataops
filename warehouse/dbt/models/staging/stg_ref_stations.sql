{{
  config(
    materialized='view',
    description='Station reference staging — id_ref_zdc (=ida in validations) + coordinates. Combines emplacement-des-gares-idf with manual complement seed for missing stations.'
  )
}}

-- Primary source: IDFM emplacement-des-gares-idf (1240 stations)
-- Complement: stop_coordinates_complement seed (23 stations missing from primary source)
-- Together: covers ~100% of validation volume

WITH primary_source AS (
  SELECT
    CAST(id_ref_zdc AS INT64)       AS station_id,
    TRIM(station_name)              AS station_name,
    CAST(latitude AS FLOAT64)       AS latitude,
    CAST(longitude AS FLOAT64)      AS longitude,
    TRIM(mode)                      AS transport_mode,
    TRIM(operator)                  AS operator,
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,
    ROW_NUMBER() OVER (
      PARTITION BY id_ref_zdc
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM {{ source('raw', 'raw_ref_stations') }}
  WHERE id_ref_zdc IS NOT NULL
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
    AND latitude BETWEEN 48.0 AND 49.5
    AND longitude BETWEEN 1.5 AND 3.5
),

complement AS (
  SELECT
    CAST(station_id_zdc AS INT64) AS station_id,
    TRIM(stop_name)               AS station_name,
    CAST(latitude AS FLOAT64)     AS latitude,
    CAST(longitude AS FLOAT64)    AS longitude,
    CAST(NULL AS STRING)          AS transport_mode,
    CAST(NULL AS STRING)          AS operator,
    CAST(NULL AS TIMESTAMP)       AS ingestion_ts
  FROM {{ ref('stop_coordinates_complement') }}
),

primary_deduped AS (
  SELECT station_id, station_name, latitude, longitude, transport_mode, operator, ingestion_ts
  FROM primary_source
  WHERE rn = 1
),

-- Complement only fills gaps — primary source takes precedence
combined AS (
  SELECT * FROM primary_deduped
  UNION ALL
  SELECT * FROM complement
  WHERE station_id NOT IN (SELECT station_id FROM primary_deduped)
)

SELECT * FROM combined
