{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT
    program, 
    region,
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
        WHEN (score IN (1)) THEN 'Engagement - A Few'
        WHEN (score IN (2)) THEN 'Engagement - About Half'
        WHEN (score IN (3)) THEN 'Engagement - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('e1') THEN 'Followed instructions'
        WHEN subindicator IN ('e2') THEN 'Participated in discussions'
        WHEN subindicator IN ('c1') THEN 'Asked questions'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Curiosity & Critical Thinking and Engagement', 'Curiosity & Critical Thinking')
    AND forms in ('cro_ug', 'cro', 'cro_indo')
    AND subindicator IN ('e1', 'e2', 'c1')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL