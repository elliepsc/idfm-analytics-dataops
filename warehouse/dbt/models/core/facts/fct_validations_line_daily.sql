{{
  config(
    materialized='incremental',
    unique_key='line_validation_key',
    partition_by={
      "field": "validation_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['line_id', 'transport_mode', 'ticket_type'],
    description='Daily validations aggregated at line level from trusted stop-to-line mappings.'
  )
}}

-- Aggregate validations at line level from the validation-stop bridge.
-- Version 1 keeps high-confidence mappings only to avoid false precision
-- at multimodal hubs such as large interchange stations.

WITH bridged AS (
  SELECT *
  FROM {{ ref('int_validation_stop_line') }}
  WHERE is_trusted_mapping = TRUE

  {% if is_incremental() %}
    {% if target.name == 'dev' %}
  AND validation_date BETWEEN '{{ var("dev_start_date") }}' AND '{{ var("dev_end_date") }}'
    {% else %}
  AND validation_date > (SELECT MAX(validation_date) FROM {{ this }})
    {% endif %}
  {% endif %}
),

aggregated AS (
  SELECT
    validation_date,
    validation_month,
    line_id,
    line_name,
    transport_mode,
    operator,
    ticket_type,
    SUM(validation_count) AS trusted_validation_count,
    COUNT(DISTINCT idfm_stop_id) AS trusted_stop_count,
    MAX(ingestion_ts) AS ingestion_ts
  FROM bridged
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
  {{ dbt_utils.generate_surrogate_key([
    'validation_date',
    'line_id',
    'ticket_type'
  ]) }} AS line_validation_key,

  validation_date,
  validation_month,
  line_id,
  line_name,
  transport_mode,
  operator,
  ticket_type,
  trusted_validation_count,
  CAST(NULL AS FLOAT64) AS estimated_validation_count,
  CAST(trusted_validation_count AS FLOAT64) AS total_validation_count,
  trusted_stop_count,
  trusted_stop_count AS all_candidate_stop_count,
  1.0 AS coverage_ratio,
  'trusted_only' AS mapping_quality,
  ingestion_ts,
  CURRENT_TIMESTAMP() AS dbt_updated_at

FROM aggregated
