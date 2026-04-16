-- Staging - Service Quality Quarterly
-- DISABLED: set enabled=True to activate.
-- ✅ READY TO ENABLE: raw_service_quality is provisioned (transport_quarterly_pipeline loaded it 2026-04-15).
-- To enable: set enabled=True here, then also enable fct_service_quality_quarterly.
{{
  config(
    materialized='view',
    enabled=True,
    description='IDFM service quality indicators — cleaned and deduplicated staging. Grain: 1 line × 1 quarter × 1 indicator.'
  )
}}

-- X1 reliability layer: service quality indicators replace punctuality-only KPI.
-- The source dataset covers RATP Métro + RER + SNCF Transilien — all major modes.
-- Quarter format from extractor: "T1 2022" (trimestre + annee combined).
-- Parsed into a DATE (first day of quarter).
-- Deduplication: keeps the most recently ingested row per (line_id, quarter_date, indicator_label).

WITH source AS (
  SELECT * FROM {{ source('raw', 'raw_service_quality') }}
),

cleaned AS (
  SELECT
    quarter,
    line_id,
    TRIM(line_name)                                            AS line_name,
    TRIM(indicator_label)                                      AS indicator_label,
    TRIM(indicator_theme)                                      AS indicator_theme,
    SAFE_CAST(indicator_value AS FLOAT64)                      AS indicator_value,
    SAFE_CAST(target_value AS FLOAT64)                         AS target_value,
    UPPER(TRIM(transport_mode))                                AS transport_mode,
    UPPER(TRIM(operator))                                      AS operator,
    CAST(ingestion_ts AS TIMESTAMP)                            AS ingestion_ts,
    source
  FROM source
  WHERE
    quarter IS NOT NULL
    AND indicator_label IS NOT NULL
    AND COALESCE(NULLIF(TRIM(line_id), ''), NULLIF(TRIM(line_name), '')) IS NOT NULL
    AND SAFE_CAST(indicator_value AS FLOAT64) IS NOT NULL
),

-- Parse "T1 2022" → DATE 2022-01-01, extract quarter_num (1-4), year
-- Format: SPLIT(' ')[0] = T1/T2/T3/T4, SPLIT(' ')[1] = year
parsed AS (
  SELECT
    *,

    -- Quarter start date: T1→Jan, T2→Apr, T3→Jul, T4→Oct
    DATE(
      CAST(SPLIT(quarter, ' ')[OFFSET(1)] AS INT64),
      CASE SPLIT(quarter, ' ')[OFFSET(0)]
        WHEN 'T1' THEN 1
        WHEN 'T2' THEN 4
        WHEN 'T3' THEN 7
        WHEN 'T4' THEN 10
        ELSE NULL
      END,
      1
    )                                                          AS quarter_date,

    CAST(REGEXP_EXTRACT(quarter, r'T(\d)') AS INT64)           AS quarter_num,
    CAST(SPLIT(quarter, ' ')[OFFSET(1)] AS INT64)              AS year

  FROM cleaned
  WHERE
    ARRAY_LENGTH(SPLIT(quarter, ' ')) = 2
    AND REGEXP_CONTAINS(quarter, r'^T[1-4] \d{4}$')
),

deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(line_id, line_name), quarter_date, indicator_label
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM parsed
)

SELECT
  quarter_date,
  quarter_num,
  year,
  quarter,
  COALESCE(line_id, line_name)                               AS line_id,
  line_name,
  indicator_label,
  indicator_theme,
  indicator_value,
  target_value,
  ROUND(indicator_value - target_value, 2)                   AS gap_to_target,

  -- Quality band relative to target
  CASE
    WHEN target_value IS NULL                    THEN 'no_target'
    WHEN indicator_value >= target_value + 2.0   THEN 'exceeds_target'
    WHEN indicator_value >= target_value          THEN 'meets_target'
    WHEN indicator_value >= target_value - 5.0   THEN 'near_target'
    ELSE                                              'below_target'
  END                                                        AS quality_band,

  transport_mode,
  operator,
  ingestion_ts,
  source

FROM deduplicated
WHERE rn = 1
