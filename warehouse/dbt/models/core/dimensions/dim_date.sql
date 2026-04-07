-- Dimension - Dates (calendar)
{{
  config(
    materialized='table',
    description='Dimension calendaire standard enrichie — labels mois FR, vacances scolaires IDF, jours fériés'
  )
}}

-- V3 3h + 3m: enriched dim_date
-- 3m: month_name_fr — Looker Studio formats month integers poorly without a label
-- 3h: is_school_holiday + is_public_holiday — corrects z-score baseline bias.
--     Without these flags, the monitoring z-score window includes holiday periods
--     (low volume baseline) which causes false anomaly alerts when traffic returns
--     to normal. is_school_holiday allows filtering the baseline to comparable days.
--
-- School holidays: Zone C (Paris IDF) — standard academic calendar 2024-2026
-- Public holidays: French national holidays

WITH base AS (
  {{
    dbt_date.get_date_dimension(
      start_date='2024-01-01',
      end_date='2026-12-31'
    )
  }}
),

enriched AS (
  SELECT
    base.*,

    -- 3m: French month name (Metal — agg_festival_seasonality pattern)
    CASE EXTRACT(MONTH FROM base.date_day)
      WHEN 1  THEN 'Janvier'
      WHEN 2  THEN 'Février'
      WHEN 3  THEN 'Mars'
      WHEN 4  THEN 'Avril'
      WHEN 5  THEN 'Mai'
      WHEN 6  THEN 'Juin'
      WHEN 7  THEN 'Juillet'
      WHEN 8  THEN 'Août'
      WHEN 9  THEN 'Septembre'
      WHEN 10 THEN 'Octobre'
      WHEN 11 THEN 'Novembre'
      WHEN 12 THEN 'Décembre'
    END AS month_name_fr,

    -- 3h: French public holidays (jours fériés nationaux)
    CASE
      WHEN FORMAT_DATE('%m-%d', base.date_day) IN (
        '01-01',  -- Jour de l'An
        '05-01',  -- Fête du Travail
        '05-08',  -- Victoire 1945
        '07-14',  -- Fête Nationale
        '08-15',  -- Assomption
        '11-01',  -- Toussaint
        '11-11',  -- Armistice
        '12-25'   -- Noël
      ) THEN TRUE
      -- Moveable feasts 2024
      WHEN base.date_day IN ('2024-04-01', '2024-05-09', '2024-05-20') THEN TRUE
      -- Moveable feasts 2025
      WHEN base.date_day IN ('2025-04-21', '2025-05-29', '2025-06-09') THEN TRUE
      -- Moveable feasts 2026
      WHEN base.date_day IN ('2026-04-06', '2026-05-14', '2026-05-25') THEN TRUE
      ELSE FALSE
    END AS is_public_holiday,

    -- 3h: IDF Zone C school holidays 2024-2026
    -- Source: Ministère de l'Éducation Nationale — calendrier scolaire officiel
    CASE
      WHEN base.date_day BETWEEN '2024-02-17' AND '2024-03-03'  THEN TRUE  -- Hiver 2024
      WHEN base.date_day BETWEEN '2024-04-13' AND '2024-04-28'  THEN TRUE  -- Printemps 2024
      WHEN base.date_day BETWEEN '2024-07-06' AND '2024-09-01'  THEN TRUE  -- Été 2024
      WHEN base.date_day BETWEEN '2024-10-19' AND '2024-11-04'  THEN TRUE  -- Toussaint 2024
      WHEN base.date_day BETWEEN '2024-12-21' AND '2025-01-05'  THEN TRUE  -- Noël 2024
      WHEN base.date_day BETWEEN '2025-02-15' AND '2025-03-02'  THEN TRUE  -- Hiver 2025
      WHEN base.date_day BETWEEN '2025-04-12' AND '2025-04-27'  THEN TRUE  -- Printemps 2025
      WHEN base.date_day BETWEEN '2025-07-05' AND '2025-09-01'  THEN TRUE  -- Été 2025
      WHEN base.date_day BETWEEN '2025-10-18' AND '2025-11-03'  THEN TRUE  -- Toussaint 2025
      WHEN base.date_day BETWEEN '2025-12-20' AND '2026-01-05'  THEN TRUE  -- Noël 2025
      WHEN base.date_day BETWEEN '2026-02-14' AND '2026-03-01'  THEN TRUE  -- Hiver 2026
      WHEN base.date_day BETWEEN '2026-04-11' AND '2026-04-26'  THEN TRUE  -- Printemps 2026
      ELSE FALSE
    END AS is_school_holiday

  FROM base
)

SELECT * FROM enriched
