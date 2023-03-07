{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

select
{{ dbt_utils.star(from= source('source_classroom_surveys', 'delhi'), except=['cro5', 'cro4', 'cro7a', 'cro8a', 'cro9', 'cro8', 'cro7', 'cro10', 'cro11', 'cro12', 'diet_delhi', 's1', 'date', 'date_coaching','starttime','endtime','submissiondate','completiondate']) }},
'India' AS country, 'delhi' AS region, diet_delhi as sub_region, to_date(coalesce(date,date_coaching), 'Mon, DD YYYY') as observation_date, COALESCE(cro10, se1) as cro10, COALESCE(cro11, se2) as cro11,
COALESCE(cro12, se3) as cro12, COALESCE(cro7, c1) as cro7, COALESCE(cro8, c2) as cro8, COALESCE(cro9, c3) as cro9, COALESCE(cro8a, c2a) as cro8a, COALESCE(cro7a, c1a) as cro7a, COALESCE(cro4, e1) as cro4, COALESCE(cro5, e2) as cro5,
to_timestamp(starttime,'Mon, DD YYYY HH:MI:SS AM') AS starttime,
to_timestamp(endtime,'Mon, DD YYYY HH:MI:SS AM') AS endtime,
to_timestamp(completiondate,'Mon, DD YYYY HH:MI:SS AM') AS completiondate,
to_timestamp(submissiondate,'Mon, DD YYYY HH:MI:SS AM') AS submissiondate
from {{ source('source_classroom_surveys', 'delhi') }} 

