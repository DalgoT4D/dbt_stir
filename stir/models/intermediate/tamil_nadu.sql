{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]

) }}

select
{{ dbt_utils.star(from= source('survey-cto-tn', 'tamil_nadu'), except=['district_tn', 's1']) }},
'India' AS country, 'tamil_nadu' AS region, district_tn as sub_region, COALESCE(s1, cro1) as s1
from {{ source('survey-cto-tn', 'tamil_nadu') }} 
