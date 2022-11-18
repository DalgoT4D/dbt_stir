{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    enabled=true


) }}

select
{{ dbt_utils.star(from= source('source_classroom_surveys', 'tamil_nadu'), except=['district_tn', 's1']) }},
'India' AS country, 'tamil_nadu' AS region, district_tn as sub_region, COALESCE(s1, cro1) as s1
from {{ source('source_classroom_surveys', 'tamil_nadu') }} 
