{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    enabled=true
) }}

select
forms,
{{ dbt_utils.star(from= source('source_classroom_surveys', 'karnataka'), except=['forms', 'district_kt', 'date', 'date_coaching','starttime','endtime','submissiondate','completiondate', '_airbyte_karnataka_stir_bm_2022_hashid']) }},
'India' AS country, 'karnataka' AS region, district_kt as sub_region, to_date(coalesce(date,date_coaching), 'Mon, DD YYYY') as observation_date,
to_timestamp(starttime,'Mon, DD YYYY HH:MI:SS AM') AS starttime,
to_timestamp(endtime,'Mon, DD YYYY HH:MI:SS AM') AS endtime,
to_timestamp(completiondate,'Mon, DD YYYY HH:MI:SS AM') AS completiondate,
to_timestamp(submissiondate,'Mon, DD YYYY HH:MI:SS AM') AS submissiondate
from {{ source('source_classroom_surveys', 'karnataka') }} 
