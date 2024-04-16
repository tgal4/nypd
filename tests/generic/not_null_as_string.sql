not_null_as_string.sql
{% test not_null_as_string(model, column_name) %}
    select {{ column_name }} 
    from {{ model }}
    where TRIM({{ column_name }}) like '%null%'
{% endtest %}