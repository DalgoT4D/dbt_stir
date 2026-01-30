{{ config(
  materialized='table',
  schema=generate_schema_name('prod_intermediate', this)
) }}

select
forms::text as forms,
{{ dbt_utils.star(from= ref('karnataka_normalized'), except=['forms', 'district_kt', 'date', 'date_coaching','starttime','endtime','submissiondate','"CompletionDate"', '_airbyte_karnataka_stir_bm_2022_hashid']) }},
'India'::text AS country, 'karnataka'::text AS region, district_kt::text as sub_region, to_date(coalesce(date,date_coaching), 'YYYY-MM-DD') as observation_date,
c1 as e3, -- Duplicate c1 as e3 for dual classification
COALESCE("remarks", "remarks_coaching") as remarks_qualitative,
starttime::timestamp AS starttime,
endtime::timestamp AS endtime,
-- to_timestamp("CompletionDate",'Mon, DD YYYY HH:MI:SS AM') AS completiondate, -- CompletionDate column does not exist in CSV
"SubmissionDate"::timestamptz AS submissiondate
from {{ ref('karnataka_normalized') }} 
