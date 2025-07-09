{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
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
        WHEN subindicator IN ('c1') THEN 'Asked questions'
        WHEN subindicator IN ('c2') THEN 'Asked critical questions'
        WHEN subindicator IN ('c3') THEN 'Related content to everyday work'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Curiosity & Critical Thinking', 'Curiosity & Critical Thinking and Engagement')
    AND forms in ('nm_indo','nm_art','nm','nm_ug','nm_coart')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL