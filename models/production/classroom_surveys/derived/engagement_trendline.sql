{{ config(
  materialized='table',
) }}

SELECT 
  "program",
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
  plname,
  education_level
FROM {{ ref('classroom_surveys_normalized') }}
WHERE 
  behavior IN ('Engagement')