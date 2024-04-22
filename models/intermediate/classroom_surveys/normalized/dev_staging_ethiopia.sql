{{ config(
  materialized='table'
) }}

{{ flatten_json(
    model_name = source('source_classroom_surveys_dev', 'ethiopia'),
    json_column = 'data'
) }}
