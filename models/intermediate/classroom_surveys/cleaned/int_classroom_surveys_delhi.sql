{{ config(
  materialized='table',
  schema=generate_schema_name('prod_intermediate', this)
) }}

select
forms,
{{ dbt_utils.star(from= ref('delhi_normalized'), except=['forms', 'diet_delhi', 's1', 's2', 's3', 'e1', 'e2','c1', 'c1a', 'c2', 'c2a', 'c3', 'se1', 'se2', 'se3', 'date', 'date_coaching','starttime','endtime','submissiondate','"CompletionDate"', '_airbyte_delhi_india__ment_form_2022_hashid']) }},
'India' AS country, 'delhi' AS region, diet_delhi as sub_region, to_date(coalesce(date,date_coaching), 'Mon, DD YYYY') as observation_date,
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
COALESCE("remarks", "remarks_classroom", "remarks_coaching") as remarks_qualitative,
"AD1" as ad1,
"AD2" as ad2,
"AD3" as ad3,
"AD4" as ad4,
"AD5" as ad5, 
"AD7" as ad7,
"AD8" as ad8,
"AD9" as ad9,
to_timestamp(starttime,'Mon, DD YYYY HH:MI:SS AM') AS starttime,
to_timestamp(endtime,'Mon, DD YYYY HH:MI:SS AM') AS endtime,
to_timestamp("CompletionDate",'Mon, DD YYYY HH:MI:SS AM') AS completiondate,
to_timestamp("SubmissionDate",'Mon, DD YYYY HH:MI:SS AM') AS submissiondate
from {{ ref('delhi_normalized') }} 

