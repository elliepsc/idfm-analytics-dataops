-- Dimension - Dates (calendar)
{{
  config(
    materialized='table',
    description='Dimension calendaire standard'
  )
}}

--  Use dbt_utils.date_spine to generate a date dimension covering the period of interest
{{
  dbt_date.get_date_dimension(
    start_date='2024-01-01',
    end_date='2026-12-31'
  )
}}
