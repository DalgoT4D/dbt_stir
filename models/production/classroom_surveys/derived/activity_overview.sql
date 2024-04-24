{{ config(
  materialized='table'
) }}

WITH filtered_merged AS (
    SELECT 
        *,
        CASE 
            WHEN forms IN (
                'asshu_nb','asshu_ins','cct_ins','sel_ins','del_ins','dpo_nb',
                'dam_ug','mid_term_ug','el_ins','elm_ins','dam','dmpc',
                'dcm_indo','ss_indo','sp_indo','dcac_indo'
            ) THEN 'Institutes'
            WHEN forms IN ('nm','nm_art','nm_coart','nm_ug','nm_indo') THEN 'Network Meetings'
            WHEN forms IN ('cro','cro_ug','cro_indo') THEN 'Classroom Observations'
            WHEN forms IN ('cc','cc_ug','cc_indo') THEN 'Coaching Calls'
            ELSE 'Other'
        END AS activity_type
    FROM 
        {{ ref('classroom_surveys_normalized') }}
)

SELECT 
    coalesce(region, 'Unknown') as region,
    SubmissionDate,
    "KEY",
    COUNT(*) FILTER (WHERE activity_type = 'Institutes') AS institutes_count,
    COUNT(*) FILTER (WHERE activity_type = 'Network Meetings') AS network_meetings_count,
    COUNT(*) FILTER (WHERE activity_type = 'Classroom Observations') AS classroom_observations_count,
    COUNT(*) FILTER (WHERE activity_type = 'Coaching Calls') AS coaching_calls_count
FROM 
    filtered_merged
GROUP BY 
    region, SubmissionDate, "KEY"
