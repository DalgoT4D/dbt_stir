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
    behavior
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE forms IN (
        'nm', 'nm_art', 'nm_coart', 'nm_ug', 'nm_indo'
    )