{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    enabled=true

) }}

select
{{ dbt_utils.star(from= source('source_classroom_surveys', 'uganda'), except=[district_bunyoro,district_kigezi,
    district_masaka, district_rwenzori, district_central]) }},
'Uganda' AS country, location_uganda AS region, coalesce (district_bunyoro,district_kigezi,
    district_masaka, district_rwenzori, district_central) as sub_region
from {{ source('source_classroom_surveys', 'uganda') }} 
