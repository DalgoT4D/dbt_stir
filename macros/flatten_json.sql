{% macro flatten_json(model_name, json_column) %}
  
{% set survey_methods_query %}
SELECT DISTINCT(jsonb_object_keys({{json_column}})) as column_name
FROM {{model_name}}
{% endset %}

{% set results = run_query(survey_methods_query) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

SELECT
{% for column_name in results_list %}
{{ json_column }}->>'{{ column_name }}' as "{{ column_name }}"{% if not loop.last %},{% endif %}
{% endfor %}
FROM {{model_name}}
{% endmacro %}
