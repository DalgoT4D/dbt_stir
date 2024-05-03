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
    AND behavior in ('Curiosity & Critical Thinking')
    AND forms in ('nm_indo','nm_art','nm','nm_ug','nm_coart')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator
