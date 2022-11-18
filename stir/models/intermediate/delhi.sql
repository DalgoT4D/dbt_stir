{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]

) }}

select
{{ dbt_utils.star(from= source('survey-cto', 'delhi'), except=['diet_delhi', 's1']) }},
'India' AS country, 'delhi' AS region, diet_delhi as sub_region, COALESCE(s1, ad1) as s1
from {{ source('survey-cto', 'delhi') }} 
