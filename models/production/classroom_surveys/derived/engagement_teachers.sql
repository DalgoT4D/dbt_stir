{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    submissiondate,
    score,
    sub_region,
    behavior,
    "KEY",
    COUNT("KEY") as count_keys,
    CASE
        WHEN (score IN (1)) THEN 'Engagement - A Few'
        WHEN (score IN (2)) THEN 'Engagement - About Half'
        WHEN (score IN (3)) THEN 'Engagement - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('e1') THEN 'Participated in discussion'
        WHEN subindicator IN ('e2') THEN 'Made action plans'
        WHEN subindicator IN ('c1') THEN 'Asked questions'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Engagement', 'Curiosity & Critical Thinking')
    AND forms in ('nm_indo','nm_art','nm','nm_ug','nm_coart')
    AND subindicator IN ('c1', 'e1', 'e2')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region
HAVING region IS NOT NULL OR sub_region IS NOT NULL