{{ config(
  materialized='table',
  schema=generate_schema_name('prod_intermediate', this)
) }}

SELECT *
FROM {{ source('source_classroom_surveys', 'indonesia') }}