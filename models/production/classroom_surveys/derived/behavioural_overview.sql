{{ config(
  materialized='table'
) }}

WITH filtered_data AS (
    SELECT 
        *,
        CASE 
            WHEN behavior IN ('Curiosity & Critical Thinking', 'Engagement', 'Safety', 'Self Esteem') AND score IN (1, 2, 3) THEN 'Behavioral Overview'
            WHEN behavior = 'Curiosity & Critical Thinking' AND score IN (1, 2, 3) THEN 'Curiosity & Critical Thinking'
            WHEN behavior = 'Engagement' THEN 'Engagement'
            WHEN behavior = 'Safety' THEN 'Safety'
            WHEN behavior = 'Self Esteem' THEN 'Self Esteem'
            ELSE 'Other'
        END AS behavior_type
    FROM 
        {{ ref('classroom_surveys_normalized') }}
)

SELECT 
    coalesce(region, 'Unknown') as region,
    SubmissionDate,
    "KEY",
    COUNT(*) FILTER (WHERE behavior_type = 'Behavioral Overview') AS behavioral_overview_count,
    COUNT(*) FILTER (WHERE behavior_type = 'Curiosity & Critical Thinking') AS curiosity_critical_thinking_count,
    COUNT(*) FILTER (WHERE behavior_type = 'Engagement') AS engagement_count,
    COUNT(*) FILTER (WHERE behavior_type = 'Safety') AS safety_count,
    COUNT(*) FILTER (WHERE behavior_type = 'Self Esteem') AS self_esteem_count
FROM 
    filtered_data
GROUP BY 
    region, SubmissionDate, "KEY"
