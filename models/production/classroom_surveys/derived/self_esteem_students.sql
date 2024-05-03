{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    coalesce(region, 'Unknown') as region,
    submissiondate,
    score,
    behavior,
    "KEY",
    COUNT("KEY") as count_keys,
    CASE
        WHEN (score IN (1)) THEN 'Self Esteem - A Few'
        WHEN (score IN (2)) THEN 'Self Esteem - About Half'
        WHEN (score IN (3)) THEN 'Self Esteem - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('se1') THEN 'Sought teacher support'
        WHEN subindicator IN ('se2') THEN 'Conducted assigned tasks'
        WHEN subindicator IN ('se3') THEN 'Worked independently'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Self Esteem')
    AND forms in ('cro_ug', 'cro', 'cro_indo')
    AND subindicator IN ('se1', 'se2', 'se3')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator
