all_metrics AS (
  SELECT * FROM validations_metrics
  UNION ALL
  SELECT * FROM punctuality_metrics
  UNION ALL
  SELECT * FROM scorecard_metrics
),

-- ─────────────────────────────────────────────────────────────
-- Application des SLA
-- ─────────────────────────────────────────────────────────────
final AS (
  SELECT
    m.table_name,
    m.metric_date,
    m.row_count,
    m.freshness_hours,
    m.null_percentage,
    m.duplicate_count,
    s.sla_hours,
    (m.freshness_hours <= s.sla_hours AND m.row_count > 0) AS sla_met
  FROM all_metrics m
  LEFT JOIN sla_config s
    ON m.table_name = s.table_name
)

SELECT * FROM final
