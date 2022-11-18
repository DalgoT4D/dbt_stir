{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    enabled=true

) }}

select
{{ dbt_utils.star(from= source('source_classroom_surveys', 'indonesia'), except=['district_indonesia']) }},
'Indonesia' AS country, location_indonesia AS region, district_indonesia as sub_region
from {{ source('source_classroom_surveys', 'indonesia') }} 
