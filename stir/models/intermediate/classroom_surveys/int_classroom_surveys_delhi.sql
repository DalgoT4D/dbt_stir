{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]

) }}

select
{{ dbt_utils.star(from= source('source_classroom_surveys', 'delhi'), except=['diet_delhi', 's1']) }},
'India' AS country, 'delhi' AS region, diet_delhi as sub_region
from {{ source('source_classroom_surveys', 'delhi') }} 
