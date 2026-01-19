{{ config(
  materialized='table',
) }}

SELECT 
  "KEY",
  "submissiondate",
  "observation_date",
  "remarks_qualitative",
  "country",
  "region",
  "sub_region",
  "forms",
  "forms_verbose",
  "observation_term",
  "meeting",
  "role_coaching",
  behavior,
  subindicator,
  score,
  program,
  plname,
  education_level
FROM {{ ref('classroom_surveys_normalized') }}
WHERE 
  behavior = 'Self Esteem'