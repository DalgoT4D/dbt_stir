{{ config(
  materialized='table'
) }}

SELECT 
    coalesce(region, 'Unknown') as region,
    submissiondate,
    score,
    behavior,
    "KEY",
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior = 'Curiosity & Critical Thinking' AND score IN (1, 2, 3)) AS curiosity_critical_thinking_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior = 'Engagement' AND score IN (1, 2, 3)) AS engagement_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior = 'Safety' AND score IN (1, 2, 3)) AS safety_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior = 'Self Esteem' AND score IN (1, 2, 3)) AS self_esteem_count,
    COUNT(DISTINCT "KEY") FILTER (
        WHERE (
            (behavior = 'Curiosity & Critical Thinking' AND score IN (1, 2, 3)) OR
            (behavior = 'Engagement' AND score IN (1, 2, 3)) OR
            (behavior = 'Safety' AND score IN (1, 2, 3)) OR
            (behavior = 'Self Esteem' AND score IN (1, 2, 3))
        )
    ) AS behavioral_overview_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior = 'Intentional Teaching' AND score >= 0) AS intentional_teaching_count
FROM 
    {{ ref('classroom_surveys_normalized') }}
GROUP BY 
    region, submissiondate, "KEY", behavior, score
