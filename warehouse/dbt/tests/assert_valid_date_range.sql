{% test assert_valid_date_range(model, column_name, min_date='2020-01-01') %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} < CAST('{{ min_date }}' AS DATE)

{% endtest %}
