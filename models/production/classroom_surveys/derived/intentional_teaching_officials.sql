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
        WHEN (score IN (1.0)) THEN 'Yes'
        WHEN (score IN (0.0)) THEN 'No'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('cc1') THEN 'Readily provides inputs'
        WHEN subindicator IN ('cc2') THEN 'Links to goals'
        WHEN subindicator IN ('cc3') THEN 'Lists action points'
        WHEN subindicator IN ('cc4') THEN 'Problem-solving approach'
        WHEN subindicator IN ('cc5') THEN 'Asks questions'
        ELSE 'Other'
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Intentional Teaching')
    AND score <= 1.0
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator
