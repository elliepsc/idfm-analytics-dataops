{% macro primary_transport_mode(stop_id_column) %}
    -- V3 3l: primary_transport_mode macro (Steam — genre_priority_seed pattern)
    -- Resolves multimodal stations (RER + Metro + Transilien serving same stop)
    -- to a single primary mode using priority order from transport_mode_priority seed.
    --
    -- Usage: {{ primary_transport_mode('stop_id') }} AS primary_transport_mode
    -- Used in: mart_validations_station_daily, Looker Studio Station Map (Page 5)
    --
    -- Priority: RER(1) > METRO(2) > TRANSILIEN(3) > TRAM(4) > ... > BUS(7)
    -- A stop served by both RER and METRO → primary = RER

    (
      SELECT
        sl.transport_mode
      FROM {{ ref('stg_ref_stop_lines') }} sl
      INNER JOIN {{ ref('stg_ref_lines') }} l
        ON sl.line_id = l.line_id
      INNER JOIN {{ ref('transport_mode_priority') }} p
        ON UPPER(TRIM(l.transport_mode)) = UPPER(TRIM(p.transport_mode))
      WHERE sl.stop_id = {{ stop_id_column }}
      ORDER BY p.priority ASC
      LIMIT 1
    )

{% endmacro %}
