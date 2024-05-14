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
    "KEY",
    COUNT("KEY") as count_keys,
    CASE
        WHEN (score IN (1)) THEN 'Safety - A Few'
        WHEN (score IN (2)) THEN 'Safety - About Half'
        WHEN (score IN (3)) THEN 'Safety - Most'
        ELSE 'Other'
    END AS score_category,
    CASE 
        WHEN subindicator IN ('s1') THEN 'Shared relevant, real-life examples'
        WHEN subindicator IN ('s2') THEN 'Received feedback'
        WHEN subindicator IN ('s3') THEN 'Practiced a strategy'
        WHEN subindicator IN ('s4') THEN 'Referred to data'
        ELSE 'Other' 
    END AS subindicator_category
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE 
    score is not NULL
    AND behavior in ('Safety')
    AND forms in ('dc_ins ', 'el_ins', 'elm_ins', 'dmpc', 'dam', 'dpo_nb', 'asshu_nb', 'cct_ins', 'del_ins', 'sel_ins', 'nm_ug', 'nm', 'nm_art', 'nm_coart', 'dcm_indo', 'dcac_indo', 'ss_indo', 'sp_indo', 'nm_indo', 'dam_ug', 'midterm_ug')
GROUP BY 
    region, submissiondate, "KEY", behavior, score, subindicator, sub_region, country, forms
HAVING region IS NOT NULL AND sub_region IS NOT NULL