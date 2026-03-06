{% test assert_positive_values(model, column_name) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} < 0

{% endtest %}
