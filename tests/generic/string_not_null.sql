string_not_null.sql
{% test string_not_null(model, column_name) %}
    select {{ column_name }} 
    from {{ model }}
    where {{ column_name }} is null
{% endtest %}