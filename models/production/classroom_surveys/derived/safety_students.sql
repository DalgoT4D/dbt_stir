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
        WHEN (behavior = 'Safety') AND (score IN (1)) THEN 'Safety - Most'
        WHEN (behavior = 'Safety') AND (score IN (2)) THEN 'Safety - About Half'
        WHEN (behavior = 'Safety') AND (score IN (3)) THEN 'Safety - A Few'
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
    region, submissiondate, "KEY", behavior, score, subindicator
