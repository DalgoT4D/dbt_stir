{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT
    program, 
    region,
    submissiondate,
    country,
    "KEY",
    score,
    forms,
    sub_region,
    behavior,
    malepresent,
    femalepresent,
    plname,
    education_level,
    COUNT(DISTINCT "KEY") FILTER (WHERE forms IN (
        'asshu_nb','asshu_ins','cct_ins','sel_ins','del_ins','dpo_nb',
        'dam_ug','mid_term_ug','el_ins','elm_ins','dam','dmpc',
        'dcm_indo','ss_indo','sp_indo','dcac_indo'
    )) AS institutes_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE forms IN (
        'nm', 'nm_art', 'nm_coart', 'nm_ug', 'nm_indo'
    )) AS network_meetings_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE forms IN (
        'cro', 'cro_ug', 'cro_indo'
    )) AS classroom_observations_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE forms IN (
        'cc', 'cc_ug', 'cc_indo'
    )) AS coaching_calls_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior IN (
        'Curiosity & Critical Thinking'
    ) AND score IN (1, 2, 3)) AS c_and_ct_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior IN (
        'Engagement')) AS engagement_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior IN (
        'Safety')) AS safety_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE behavior IN (
        'Self Esteem')) AS self_esteem_count
FROM 
    {{ ref('classroom_surveys_normalized') }}
GROUP BY 
    region, submissiondate, "KEY", forms, sub_region, score, country, behavior, malepresent,
    femalepresent, program, plname, education_level
