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
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND behavior = 'Safety' AND forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')) AS safety_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND behavior IN ('Engagement', 'Curiosity & Critical Thinking') AND subindicator IN ('c1', 'e1', 'e2') AND forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')) AS engagement_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND behavior = 'Curiosity & Critical Thinking' AND forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')) AS curiosity_critical_thinking_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IN (1, 2, 3) AND behavior = 'Self Esteem' AND forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')) AS self_esteem_count
FROM 
    {{ ref('classroom_surveys_normalized') }}
GROUP BY 
    region, submissiondate, "KEY", behavior, score
