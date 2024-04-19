{{ config(
  materialized='table'
) }}

{{ flatten_json(
    model_name = source('source_classroom_surveys1', 'tamil_nadu'),
    json_column = 'data'
) }}
