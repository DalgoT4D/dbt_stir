{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    submissiondate,
    score,
    program,
    forms,
    behavior,
    sub_region,
    country,
    "KEY",
    plname,
    education_level,
    observation_term,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND behavior = 'Safety' AND forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')) AS safety_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND behavior IN ('Engagement') AND subindicator IN ('e3', 'e1', 'e2') AND forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')) AS engagement_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND behavior IN ('Curiosity & Critical Thinking') AND forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')) AS curiosity_critical_thinking_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IN (1, 2, 3) AND behavior = 'Self Esteem' AND forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')) AS self_esteem_count
FROM 
    {{ ref('classroom_surveys_normalized') }}
GROUP BY 
    region, submissiondate, "KEY", behavior, score, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL