{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]

) }}

select
{{ dbt_utils.star(from= source('survey-cto-indo', 'indonesia'), except=['district_indonesia']) }},
'Indonesia' AS country, location_indonesia AS region, district_indonesia as sub_region
from {{ source('survey-cto-indo', 'indonesia') }} 
