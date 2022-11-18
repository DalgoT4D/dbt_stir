{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    enabled=false


) }}

select
{{ dbt_utils.star(from= source('survey-cto', 'source_classroom_surveys_delhi'), except=['district_kt', 'c1']) }},
'India' AS country, 'karnataka' AS region, district_kt as sub_region, COALESCE(c1, cc1) as c1
from {{ source('survey-cto', 'source_classroom_surveys_delhi') }} 
