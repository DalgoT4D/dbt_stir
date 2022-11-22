{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    enabled=true,
    schema='intermediate'

) }}

select
{{ dbt_utils.star(from= source('source_classroom_surveys', 'karnataka'), except=['district_kt', 'c1', 'date', 'date_coaching']) }},
'India' AS country, 'karnataka' AS region, district_kt as sub_region, to_date(coalesce(date,date_coaching), 'Mon, DD YYYY') as observation_date
from {{ source('source_classroom_surveys', 'karnataka') }} 
