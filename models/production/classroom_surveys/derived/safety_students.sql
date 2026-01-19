{{ config(
  materialized='table',
) }}

SELECT 
    region,
    program,
    submissiondate,
    score,
    forms,
    sub_region,
    behavior,
    country,
    "KEY",
    plname,
    education_level,
    COUNT("KEY") as count_keys,
    observation_term,
    CASE
        WHEN (score IN (3)) THEN 'Safety - Most'
        WHEN (score IN (2)) THEN 'Safety - About Half'
        WHEN (score IN (1)) THEN 'Safety - A Few'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('s1') THEN 'Not punished physically'
        WHEN subindicator IN ('s2') THEN 'Not ridiculed/yelled at'
        WHEN subindicator IN ('s3') THEN 'Acknowledged for effort'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Safety')
    AND forms in ('cro_ug', 'cro', 'cro_indo')
    AND subindicator IN ('s1', 's2', 's3')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL