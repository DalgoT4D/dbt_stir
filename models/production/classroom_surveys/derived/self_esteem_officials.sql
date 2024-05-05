{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    submissiondate,
    score,
    sub_region,
    behavior,
    "KEY",
    COUNT("KEY") as count_keys,
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
    AND forms in ('cc', 'dmpc', 'dam', 'cc_ug', 'el_ins', 'elm_ins', 'del_ins', 'sel_ins', 'dam_ug', 'duo_nb', 'dcm_indo', 'cc_indo', 'cat_ins', 'midterm_ug', 'dc_ins', 'sash_nb')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region
HAVING region IS NOT NULL OR sub_region IS NOT NULL