{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

select
{{ dbt_utils.star(from= source('source_classroom_surveys', 'delhi'), except=['diet_delhi', 's1', 'date', 'date_coaching']) }},
'India' AS country, 'delhi' AS region, diet_delhi as sub_region, to_date(coalesce(date,date_coaching), 'Mon, DD YYYY') as observation_date
from {{ source('source_classroom_surveys', 'delhi') }} 

