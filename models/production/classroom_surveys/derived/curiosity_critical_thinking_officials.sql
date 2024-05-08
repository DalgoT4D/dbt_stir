{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    sub_region,
    submissiondate,
    forms,
    score,
    behavior,
    country,
    "KEY",
    COUNT("KEY") as count_keys,
    CASE
        WHEN (score IN (1)) THEN 'C&CT - A Few'
        WHEN (score IN (2)) THEN 'C&CT - About Half'
        WHEN (score IN (3)) THEN 'C&CT - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('c1') THEN 'Asked questions'
        WHEN subindicator IN ('c2') THEN 'Asked critical questions'
        WHEN subindicator IN ('c3') THEN 'Related content to everyday work'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Curiosity & Critical Thinking')
    AND forms in ('cc', 'dmpc', 'dam', 'cc_ug', 'el_ins', 'elm_ins', 'del_ins', 'sel_ins', 'dam_ug', 'duo_nb', 'dcm_indo', 'cc_indo', 'cat_ins', 'midterm_ug', 'dc_ins', 'sash_nb')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms
HAVING region IS NOT NULL AND sub_region IS NOT NULL