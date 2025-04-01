{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    program,
    sub_region,
    submissiondate,
    forms,
    score,
    behavior,
    country,
    plname,
    education_level,
    observation_term,
    "KEY",
    COUNT("KEY") as count_keys,
    CASE
        WHEN (score IN (1)) THEN 'C&CT - A Few'
        WHEN (score IN (2)) THEN 'C&CT - About Half'
        WHEN (score IN (3)) THEN 'C&CT - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('c1') THEN 'Followed instructions'
        WHEN subindicator IN ('c2') THEN 'Asked critical questions'
        WHEN subindicator IN ('c3') THEN 'Reflected on the lesson'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Curiosity & Critical Thinking')
    AND forms in ('cro_ug', 'cro', 'cro_indo')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL