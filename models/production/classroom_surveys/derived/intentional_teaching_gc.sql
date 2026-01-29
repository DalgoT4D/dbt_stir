{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    submissiondate,
    score,
    forms,
    sub_region,
    behavior,
    country,
    program,
    plname,
    education_level,
    observation_term,
    "KEY",
    COUNT("KEY") as count_keys,
    CASE
        WHEN (score IN (1)) THEN 'GC - A Few'
        WHEN (score IN (2)) THEN 'GC - About Half'
        WHEN (score IN (3)) THEN 'GC - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('gc1') THEN 'Provide inputs without excessive prompting'
        WHEN subindicator IN ('gc2') THEN 'Linked their planned actions to a wider purpose'
        WHEN subindicator IN ('gc3') THEN 'Listed specific action points to take forward'
        WHEN subindicator IN ('gc4') THEN 'Showed Problem-solving approach'
        WHEN subindicator IN ('gc5') THEN 'Asked why and how questions'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score in (1, 2, 3)
    AND forms in ('gc_indo')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL