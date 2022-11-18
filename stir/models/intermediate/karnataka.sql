{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]

) }}

select
{{ dbt_utils.star(from= source('survey-cto-kn', 'karnataka'), except=['district_kt', 'c1']) }},
'India' AS country, 'karnataka' AS region, district_kt as sub_region, COALESCE(c1, cc1) as c1
from {{ source('survey-cto-kn', 'karnataka') }} 
