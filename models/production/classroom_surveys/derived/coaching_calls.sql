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
    behavior,
    program, 
    plname,
    education_level,
    observation_term
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE forms IN (
        'cc', 'cc_ug', 'cc_indo', 'cc_eth'
    )