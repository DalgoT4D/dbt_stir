{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    submissiondate,
    score,
    forms,
    sub_region,
    behavior,
    country,
    program,
    plname, 
    education_level,
    "KEY",
    COUNT("KEY") as count_keys,
    observation_term,
    CASE
        WHEN (score IN (1)) THEN 'Self Esteem - A Few'
        WHEN (score IN (2)) THEN 'Self Esteem - About Half'
        WHEN (score IN (3)) THEN 'Self Esteem - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('se1') THEN 'Peer collaboration'
        WHEN subindicator IN ('se2') THEN 'Sought facilitator support'
        WHEN subindicator IN ('se3') THEN 'Recognition & celebration'
        WHEN subindicator IN ('se4') THEN 'Sought peer support'
        WHEN subindicator IN ('se5') THEN 'Shared developmental feedback'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score in (1, 2, 3)
    AND behavior in ('Self Esteem')
    AND forms in ('dmpc', 'dam', 
                  'el_ins', 'elm_ins', 
                  'del_ins', 'sel_ins', 'dam_ug', 
                  'dpo_nb', 'dcm_indo', 'cct_ins', 
                  'midterm_ug', 'dc_ins', 'asshu_nb', 'ss_indo', 'sp_indo', 'dcac_indo', 'sam')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL