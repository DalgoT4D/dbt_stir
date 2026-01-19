{{ config(
  materialized='table'
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
    behavior,
    plname,
    education_level,
    observation_term
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE forms IN (
        'cro', 'cro_ug', 'cro_indo', 'cro_eth'
    )