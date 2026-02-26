-- Staging - Référentiel lignes

{{
  config(
    materialized='view',
    description='Référentiel des lignes - version latest'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_ref_lines') }}
),

latest AS (
  SELECT
    -- Identifiants
    UPPER(TRIM(line_id)) AS line_id,
    TRIM(line_name) AS line_name,

    -- Classification
    UPPER(TRIM(transport_mode)) AS transport_mode,
    TRIM(operator) AS operator,

    -- Metadata
    CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts,

    -- Row number pour déduplication
    ROW_NUMBER() OVER (
      PARTITION BY UPPER(TRIM(line_id))
      ORDER BY CAST(ingestion_ts AS TIMESTAMP) DESC
    ) AS rn

  FROM source

  WHERE
    line_id IS NOT NULL
    AND line_name IS NOT NULL
)

SELECT
  line_id,
  line_name,
  transport_mode,
  operator,
  ingestion_ts
FROM latest
WHERE rn = 1
