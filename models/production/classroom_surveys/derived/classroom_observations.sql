{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    submissiondate,
    country,
    "KEY",
    score,
    forms,
    sub_region,
    program,
    behavior
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE forms IN (
        'cro', 'cro_ug', 'cro_indo', 'cro_eth'
    )