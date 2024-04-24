{{ config(
  materialized='table'
) }}

with my_cte as ({{
  flatten_json(
    model_name = source('source_classroom_surveys_dev', 'indonesia'),
    json_column = 'data'
)
}})


{{ dbt_utils.deduplicate(
    relation='my_cte',
    partition_by='"KEY"',
    order_by='"KEY" desc',
   )
}}
