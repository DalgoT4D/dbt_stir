{% macro test_is_timestamp(model, column_name) %}
    select *
    from {{ model }}
    where 
        {{ column_name }}::text !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{2}$'
{% endmacro %}
