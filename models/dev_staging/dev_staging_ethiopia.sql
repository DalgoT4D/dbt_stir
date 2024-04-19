{{ config(
  materialized='table'
) }}

{{ flatten_json(
    model_name = source('source_classroom_surveys1', 'ethiopia'),
    json_column = 'data'
) }}
