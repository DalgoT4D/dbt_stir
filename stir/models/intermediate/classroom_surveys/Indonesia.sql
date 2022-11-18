{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    enabled=false

) }}

select
{{ dbt_utils.star(from= source('survey-cto', 'source_classroom_surveys_indonesia'), except=['district_indonesia']) }},
'Indonesia' AS country, location_indonesia AS region, district_indonesia as sub_region
from {{ source('survey-cto', 'source_classroom_surveys_indonesia') }} 
