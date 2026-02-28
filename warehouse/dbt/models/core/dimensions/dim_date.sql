-- Dimension - Dates (calendrier)
{{
  config(
    materialized='table',
    description='Dimension calendaire standard'
  )
}}

-- Utilise le package dbt_date pour générer un calendrier complet
{{
  dbt_date.get_date_dimension(
    start_date='2024-01-01',
    end_date='2026-12-31'
  )
}}
