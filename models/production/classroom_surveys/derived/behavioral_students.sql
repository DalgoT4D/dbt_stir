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
    subindicator,
    plname,
    education_level,
    observation_term,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior = 'Safety' AND forms IN ('cro_ug', 'cro', 'cro_indo') AND subindicator IN ('s1', 's2', 's3') AND score IS NOT NULL) AS safety_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior IN ('Engagement') AND forms IN ('cro_ug', 'cro', 'cro_indo') AND subindicator IN ('e1', 'e2', 'e3') AND score IS NOT NULL) AS engagement_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior IN ('Curiosity & Critical Thinking') AND forms IN ('cro_ug', 'cro', 'cro_indo') AND score IS NOT NULL) AS curiosity_critical_thinking_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior = 'Self Esteem' AND forms IN ('cro_ug', 'cro', 'cro_indo') AND subindicator IN ('se1', 'se2', 'se3') AND score IS NOT NULL) AS self_esteem_count
FROM 
    {{ ref('classroom_surveys_normalized') }}
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL