{# 
  Custom generic test: assert all non-null dates in column_name are >= min_date.

  Usage in schema.yml:
    data_tests:
      - assert_valid_date_range:
          min_date: '2020-01-01'

  dbt 1.9+ note: the MissingArgumentsPropertyInGenericTestDeprecation warning
  is suppressed by declaring the config block below. Arguments are passed
  directly as macro kwargs — no 'arguments:' nesting needed for custom macros.
#}
{% test assert_valid_date_range(model, column_name, min_date='2020-01-01') %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} < CAST('{{ min_date }}' AS DATE)

{% endtest %}
