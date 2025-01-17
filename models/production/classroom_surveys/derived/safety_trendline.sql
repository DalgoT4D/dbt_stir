{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
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
  program
FROM {{ ref('classroom_surveys_normalized') }}
WHERE 
  behavior = 'Safety'