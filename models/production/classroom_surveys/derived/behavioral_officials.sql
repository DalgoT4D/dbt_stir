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
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND behavior = 'Safety' AND forms IN ('dc_ins ', 'el_ins', 'elm_ins', 'dmpc', 'dam', 'dpo_nb', 'asshu_nb', 'cct_ins', 'del_ins', 'sel_ins', 'nm_ug', 'nm', 'nm_art', 'nm_coart', 'dcm_indo', 'dcac_indo', 'ss_indo', 'sp_indo', 'nm_indo', 'dam_ug', 'midterm_ug')) AS safety_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND (behavior = 'Engagement' OR behavior = 'Curiosity & Critical Thinking') AND subindicator IN ('c1', 'e1', 'e2') AND forms IN ('cc', 'dmpc', 'dam', 'cc_ug', 'el_ins', 'elm_ins', 'del_ins', 'sel_ins', 'dam_ug', 'duo_nb', 'dcm_indo', 'cc_indo', 'cat_ins', 'midterm_ug', 'dc_ins', 'sash_nb')) AS engagement_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IS NOT NULL AND behavior = 'Curiosity & Critical Thinking' AND forms IN ('cro_ug', 'cro', 'cro_indo')) AS curiosity_critical_thinking_count,
    COUNT(DISTINCT "KEY") FILTER (WHERE score IN (1, 2, 3) AND behavior = 'Self Esteem' AND forms IN ('cc', 'dmpc', 'dam', 'cc_ug', 'el_ins', 'elm_ins', 'del_ins', 'sel_ins', 'dam_ug', 'duo_nb', 'dcm_indo', 'cc_indo', 'cat_ins', 'midterm_ug', 'dc_ins', 'sash_nb')) AS self_esteem_count
FROM 
    {{ ref('classroom_surveys_normalized') }}
GROUP BY 
    region, submissiondate, "KEY", behavior, score
