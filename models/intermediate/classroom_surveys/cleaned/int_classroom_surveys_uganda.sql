{{ config(
  materialized='table',
  schema=generate_schema_name('prod_intermediate', this)
) }}

select
forms_uganda::text as forms,
{{ dbt_utils.star(from= ref('uganda_normalized'), 
except=['location_uganda', 'district_bunyoro', 
'district_kigezi', 
'district_masaka', 
'district_rwenzori', 
'district_central', 
'district_mbale', 
'district_acholi',
'district_busoga', 
'district_karamoja', 
'district_lango', 
 
'district_teso', 
'district_westnile', 's1', 's2', 's3', 'e1', 'e2','c1', 'c1a', 'c2', 'c2a', 'c3', 'se1', 'se2', 'se3', 'date', 'date_coaching','starttime','endtime','submissiondate','"CompletionDate"', '_airbyte_uganda_bm_2022_hashid']) }},
'Uganda'::text AS country, location_uganda::text AS region, coalesce (district_bunyoro,district_kigezi,district_masaka, district_rwenzori, district_central, district_mbale, district_acholi, district_busoga, district_karamoja, district_lango, district_teso, district_westnile)::text as sub_region, -- Removed district_midwestern as it doesn't exist in CSV
CASE
  WHEN coalesce(date,date_coaching) IS NOT NULL AND coalesce(date,date_coaching) ~ '^\d{2}/\d{2}/\d{4}'
    THEN to_date(coalesce(date,date_coaching), 'DD/MM/YYYY')
  WHEN coalesce(date,date_coaching) IS NOT NULL
    THEN to_date(coalesce(date,date_coaching), 'YYYY-MM-DD')
  ELSE NULL
END as observation_date,
COALESCE(cro1, s1) as s1,
COALESCE(cro2, s2) as s2,  
COALESCE(cro3, s3) as s3, 
COALESCE(cro4, e1) as e1, 
COALESCE(cro5, e2) as e2, 
COALESCE(cro7, c1) as e3, -- Duplicate c1 as e3 for dual classification 
COALESCE(cro7, c1) as c1,
COALESCE(cro7a, c1a) as c1a, 
COALESCE(cro8, c2) as c2, 
COALESCE(cro8a, c2a) as c2a, 
COALESCE(cro9, c3) as c3, 
COALESCE(cro10, se1) as se1,
COALESCE(cro11, se2) as se2, 
COALESCE(cro12, se3) as se3, 
COALESCE("remarks", "remarks_classroom", "remarks_coaching") as remarks_qualitative,
CASE
  WHEN btrim(starttime) ~ '^[0-9]+(\.[0-9]+)?$'
    THEN (timestamp '1899-12-30' + (btrim(starttime)::double precision * interval '1 day'))
  WHEN starttime IS NOT NULL AND starttime ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
    THEN to_timestamp(btrim(starttime), 'DD/MM/YYYY HH24:MI:SS')
  WHEN starttime IS NOT NULL AND starttime ~ '^\d{2}/\d{2}/\d{4}'
    THEN to_timestamp(btrim(starttime), 'DD/MM/YYYY')
  WHEN starttime IS NOT NULL
    THEN starttime::timestamp
  ELSE NULL
END AS starttime,
CASE
  WHEN btrim(endtime) ~ '^[0-9]+(\.[0-9]+)?$'
    THEN (timestamp '1899-12-30' + (btrim(endtime)::double precision * interval '1 day'))
  WHEN endtime IS NOT NULL AND endtime ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
    THEN to_timestamp(btrim(endtime), 'DD/MM/YYYY HH24:MI:SS')
  WHEN endtime IS NOT NULL AND endtime ~ '^\d{2}/\d{2}/\d{4}'
    THEN to_timestamp(btrim(endtime), 'DD/MM/YYYY')
  WHEN endtime IS NOT NULL
    THEN endtime::timestamp
  ELSE NULL
END AS endtime,
-- to_timestamp("CompletionDate",'Mon, DD YYYY HH:MI:SS AM') AS completiondate, -- CompletionDate column does not exist in CSV
-- SubmissionDate from SurveyCTO; _submission_time from Kobo (submissiontime field), cast to timestamptz for downstream
(CASE
  WHEN "SubmissionDate" IS NOT NULL AND "SubmissionDate" ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
    THEN to_timestamp("SubmissionDate", 'DD/MM/YYYY HH24:MI:SS')
  WHEN "SubmissionDate" IS NOT NULL AND "SubmissionDate" ~ '^\d{2}/\d{2}/\d{4}'
    THEN to_timestamp("SubmissionDate", 'DD/MM/YYYY')
  WHEN "SubmissionDate" IS NOT NULL
    THEN "SubmissionDate"::timestamp
  WHEN btrim(_submission_time) ~ '^[0-9]+(\.[0-9]+)?$'
    THEN (timestamp '1899-12-30' + (btrim(_submission_time)::double precision * interval '1 day'))
  WHEN _submission_time IS NOT NULL AND _submission_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
    THEN to_timestamp(btrim(_submission_time), 'DD/MM/YYYY HH24:MI:SS')
  WHEN _submission_time IS NOT NULL AND _submission_time ~ '^\d{2}/\d{2}/\d{4}'
    THEN to_timestamp(btrim(_submission_time), 'DD/MM/YYYY')
  WHEN _submission_time IS NOT NULL
    THEN btrim(_submission_time)::timestamp
  ELSE NULL
END)::timestamptz AS submissiondate
from {{ ref('uganda_normalized') }} 
