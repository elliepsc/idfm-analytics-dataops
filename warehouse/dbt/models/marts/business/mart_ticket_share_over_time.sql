{{
  config(
    materialized='table',
    description='Monthly ticket category share of total network validations. Grain: 1 category x 1 month. Key insight: during disruptions, Occasionnel share collapses while Abonnement resists — structural dependency signal.'
  )
}}

-- V3 3d: mart_ticket_share_over_time
-- Business question: How does the composition of ticket types evolve over time?
--   During disruptions, Occasionnel share drops sharply — occasional riders avoid
--   the network. Abonnement riders keep validating (captive demand).
--   This is the most original analytical insight in the IDFM project.
--
-- Source: fct_validations_daily + ticket_type_mapping seed
-- Pattern: Metal — agg_subgenre_share_over_time (part/total par période)

WITH monthly_totals AS (
  SELECT
    DATE_TRUNC(validation_date, MONTH) AS validation_month,
    SUM(validation_count)              AS network_total_validations
  FROM {{ ref('fct_validations_daily') }}
  GROUP BY 1
),

monthly_by_category AS (
  SELECT
    DATE_TRUNC(f.validation_date, MONTH) AS validation_month,
    t.category,
    SUM(f.validation_count)              AS category_validations
  FROM {{ ref('fct_validations_daily') }} f
  LEFT JOIN {{ ref('ticket_type_mapping') }} t
    ON UPPER(TRIM(f.ticket_type)) = UPPER(TRIM(t.ticket_type_code))
  GROUP BY 1, 2
),

enriched AS (
  SELECT
    c.validation_month,
    COALESCE(c.category, 'Autre')           AS category,
    c.category_validations,
    t.network_total_validations,

    -- Share of total (core interpretive metric)
    ROUND(
      {{ safe_divide('c.category_validations', 't.network_total_validations') }} * 100,
      2
    ) AS category_share_pct,

    -- Month-over-month share change
    ROUND(
      {{ safe_divide('c.category_validations', 't.network_total_validations') }} * 100
      - LAG(
          {{ safe_divide('c.category_validations', 't.network_total_validations') }} * 100,
          1
        ) OVER (
          PARTITION BY COALESCE(c.category, 'Autre')
          ORDER BY c.validation_month
        ),
      2
    ) AS share_mom_change_pp

  FROM monthly_by_category c
  INNER JOIN monthly_totals t ON c.validation_month = t.validation_month
)

SELECT
  validation_month,
  category,
  category_validations,
  network_total_validations,
  category_share_pct,
  share_mom_change_pp,
  -- Signal flag: sharp drop in Occasionnel often signals network disruption
  CASE
    WHEN category = 'Occasionnel'
     AND share_mom_change_pp < -3.0
    THEN TRUE
    ELSE FALSE
  END AS disruption_signal_flag,
  CURRENT_TIMESTAMP() AS dbt_updated_at
FROM enriched
ORDER BY validation_month, category
