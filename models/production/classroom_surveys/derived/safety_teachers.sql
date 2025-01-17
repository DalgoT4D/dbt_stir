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
    "KEY",
    COUNT("KEY") as count_keys,
    CASE
        WHEN (score IN (1)) THEN 'Safety - A Few'
        WHEN (score IN (2)) THEN 'Safety - About Half'
        WHEN (score IN (3)) THEN 'Safety - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('s1') THEN 'Shared relevant, real-life examples'
        WHEN subindicator IN ('s2') THEN 'Received feedback'
        WHEN subindicator IN ('s3') THEN 'Practiced a strategy'
        WHEN subindicator IN ('s4') THEN 'Referred to data'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Safety')
    AND forms in ('nm_indo','nm_art','nm','nm_ug','nm_coart')
    AND subindicator IN ('s1', 's2', 's3')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program
HAVING region IS NOT NULL AND sub_region IS NOT NULL