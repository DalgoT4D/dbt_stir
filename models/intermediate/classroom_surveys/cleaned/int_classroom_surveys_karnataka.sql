{{ config(
  materialized='table',
  schema=generate_schema_name('prod_intermediate', this)
) }}

select
forms,
{{ dbt_utils.star(from= ref('karnataka_normalized'), except=['forms', 'district_kt', 'date', 'date_coaching','starttime','endtime','submissiondate', '_airbyte_karnataka_stir_bm_2022_hashid']) }},
'India' AS country, 'karnataka' AS region, district_kt as sub_region, to_date(coalesce(date,date_coaching), 'YYYY-MM-DD') as observation_date,
COALESCE("remarks", "remarks_coaching") as remarks_qualitative,
to_timestamp(starttime,'YYYY-MM-DD') AS starttime,
to_timestamp(endtime,'YYYY-MM-DD') AS endtime,
to_timestamp("SubmissionDate",'YYYY-MM-DD') AS submissiondate
from {{ ref('karnataka_normalized') }} 
