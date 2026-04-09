-- Staging model for TN_PA stop ID mapping
-- Bridges fct_validations_daily (STIF stop_id) ↔ stg_ref_stop_lines (IDFM stop_id)
--
-- Source: object_codes_extension.txt from IDFM GTFS feed, system TN_PA
-- TN_PA = Transport en commun Numérique / Point d'Arrêt
--       = STIF numeric stop codes used in IDFM validation data
--
-- Coverage: 528 mappings, ~98.3% of validation rows (stops 1-1633)
-- Remaining 1.7%: bus/OPTILE stops not referenced in GTFS IDFM
{{
  config(
    materialized='view',
    description='TN_PA mapping: STIF numeric stop code ↔ IDFM GTFS stop_id. Bridges validations (STIF) to stop-line pairs (IDFM).'
  )
}}

WITH source AS (
    SELECT
        idfm_stop_id,
        stif_stop_code,
        ingestion_ts
    FROM {{ source('raw', 'raw_ref_stop_id_mapping') }}
),

cleaned AS (
    SELECT
        UPPER(TRIM(idfm_stop_id)) AS idfm_stop_id,
        TRIM(stif_stop_code)      AS stif_stop_code,
        CAST(ingestion_ts AS TIMESTAMP) AS ingestion_ts
    FROM source
    WHERE idfm_stop_id IS NOT NULL
      AND stif_stop_code IS NOT NULL
      AND stif_stop_code != ''
)

SELECT * FROM cleaned
