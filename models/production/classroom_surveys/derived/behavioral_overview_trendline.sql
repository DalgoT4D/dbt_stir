{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
  "KEY",
  "program",
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
  score
FROM {{ ref('classroom_surveys_normalized') }}
WHERE 
  subindicator IN ('c1','c2','c3')
  AND score IN (1, 2, 3)
  AND behavior = 'Curiosity & Critical Thinking'