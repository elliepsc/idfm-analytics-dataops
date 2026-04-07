{% macro usage_profile(ticket_type_column, day_of_week_column) %}
    -- V3 3b: Behavioural segmentation macro (Bikeshare pickup_type pattern)
    -- Centralises classification logic to avoid CASE WHEN duplication in marts.
    --
    -- Segments:
    --   commuter_weekday : subscription pass used Mon–Fri (structural network demand)
    --   student          : Imagine R (subsidised student pass, any day)
    --   casual_visitor   : occasional tickets (T+, Navigo Jour, Navigo Easy)
    --   other            : all other ticket types
    --
    -- day_of_week: BigQuery DAYOFWEEK — 1=Sunday, 2=Monday, ..., 6=Friday, 7=Saturday
    CASE
        WHEN UPPER(TRIM({{ ticket_type_column }})) = 'IMAGINE R'
            THEN 'student'
        WHEN UPPER(TRIM({{ ticket_type_column }})) IN (
                'NAVIGO', 'NAVIGO ANNUEL', 'NAVIGO SEMAINE'
             )
             AND {{ day_of_week_column }} BETWEEN 2 AND 6
            THEN 'commuter_weekday'
        WHEN UPPER(TRIM({{ ticket_type_column }})) IN (
                'T+ BILLET', 'NAVIGO JOUR', 'NAVIGO EASY'
             )
            THEN 'casual_visitor'
        ELSE 'other'
    END
{% endmacro %}
