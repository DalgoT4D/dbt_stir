{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    program,
    region,
    submissiondate,
    score,
    forms,
    sub_region,
    behavior,
    country,
    plname,
    education_level,
    "KEY",
    COUNT("KEY") as count_keys,
    CASE
        WHEN (score IN (1)) THEN 'Engagement - A Few'
        WHEN (score IN (2)) THEN 'Engagement - About Half'
        WHEN (score IN (3)) THEN 'Engagement - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('e1') THEN 'Participated in discussion'
        WHEN subindicator IN ('e2') THEN 'Made action plans'
        WHEN subindicator IN ('c1') THEN 'Asked questions'
        ELSE 'Other'
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Engagement', 'Curiosity & Critical Thinking')
    AND forms in ('dmpc', 'dam', 
                  'el_ins', 'elm_ins', 
                  'del_ins', 'sel_ins', 'dam_ug', 
                  'dpo_nb', 'dcm_indo', 'cct_ins', 
                  'midterm_ug', 'dc_ins', 'asshu_nb', 'ss_indo', 'sp_indo', 'dcac_indo', 'sam')
    AND subindicator IN ('c1', 'e1', 'e2')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms, program, plname, education_level
HAVING region IS NOT NULL AND sub_region IS NOT NULL