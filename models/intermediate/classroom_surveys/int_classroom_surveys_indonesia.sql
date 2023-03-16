{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    enabled=true,
    schema='intermediate'

) }}

select
forms_indonesia as forms,
CASE 
  When forms_indonesia = 'nm_indo' then 'network_meeting_indonesia'
  When forms_indonesia = 'cc_indo' then 'coaching_calls_indonesia'
  When forms_indonesia = 'cro_indo' then 'classroom_observation_indonesia'
  When forms_indonesia = 'dcm_indo' then 'district_coordination_meeting_indonesia'
END As forms_verbose,
{{ dbt_utils.star(from= source('source_classroom_surveys', 'indonesia'), except=['district_indonesia', 's1', 's2', 's3', 'e1', 'e2','c1', 'c1a', 'c2', 'c2a', 'c3', 'se1', 'se2', 'se3', 'date', 'date_coaching','starttime','endtime','submissiondate','completiondate', '_airbyte_indonesia_stir_bm_2022_hashid']) }},
'Indonesia' AS country, location_indonesia AS region, district_indonesia as sub_region,
COALESCE(cro1, s1) as s1,
COALESCE(cro2, s2) as s2,  
COALESCE(cro3, s3) as s3, 
COALESCE(cro4, e1) as e1, 
COALESCE(cro5, e2) as e2, 
COALESCE(cro7, c1) as c1,
COALESCE(cro7a, c1a) as c1a, 
COALESCE(cro8, c2) as c2, 
COALESCE(cro8a, c2a) as c2a, 
COALESCE(cro9, c3) as c3, 
COALESCE(cro10, se1) as se1,
COALESCE(cro11, se2) as se2, 
COALESCE(cro12, se3) as se3, 
to_date(coalesce(date,date_coaching), 'Mon, DD YYYY') as observation_date, 
to_timestamp(starttime,'Mon, DD YYYY HH:MI:SS AM') AS starttime,
to_timestamp(endtime,'Mon, DD YYYY HH:MI:SS AM') AS endtime,
to_timestamp(completiondate,'Mon, DD YYYY HH:MI:SS AM') AS completiondate,
to_timestamp(submissiondate,'Mon, DD YYYY HH:MI:SS AM') AS submissiondate
from {{ source('source_classroom_surveys', 'indonesia') }} 
