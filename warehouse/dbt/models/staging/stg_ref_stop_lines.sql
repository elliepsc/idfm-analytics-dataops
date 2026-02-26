--
 Staging - Mapping arrêts ↔ lignes

{{
  config(
    materialized='view',
    description='Mapping arrêts ↔ lignes - version latest'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_ref_stop_lines') }}
),

latest AS (
  SELECT
    UPPER(TRIM(stop_id)) AS stop_id,
    UPPER(TRIM(line_id)) AS line_id,
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,

    ROW_NUMBER() OVER (
      PARTITION BY UPPER(TRIM(stop_id)), UPPER(TRIM(line_id))
      ORDER BY CAST(ingestion_ts AS TIMESTAMP) DESC
    ) AS rn

  FROM source

  WHERE
    stop_id IS NOT NULL
    AND line_id IS NOT NULL
)

SELECT
  stop_id,
  line_id,
  ingestion_ts
FROM latest
WHERE rn = 1
