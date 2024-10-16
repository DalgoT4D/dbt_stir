{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    region,
    submissiondate,
    country,
    "KEY",
    score,
    forms,
    sub_region,
    behavior
FROM 
    {{ ref('classroom_surveys_normalized') }}
WHERE forms IN (
        'asshu_nb','asshu_ins','cct_ins','sel_ins','del_ins','dpo_nb',
        'dam_ug','mid_term_ug','el_ins','elm_ins','dam','dmpc',
        'dcm_indo','ss_indo','sp_indo','dcac_indo', 'ad_ins', 'sd_ins', 'cs_ins', 'wam', 'sam'
    )