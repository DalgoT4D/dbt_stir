{{ config(
  materialized='table',
) }}

SELECT *
FROM {{ source('source_classroom_surveys', 'tamil_nadu') }}