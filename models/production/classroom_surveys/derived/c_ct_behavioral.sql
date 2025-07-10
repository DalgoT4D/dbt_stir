{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT 
    submissiondate,
    score
FROM 
        {{ ref('classroom_surveys_normalized') }}
WHERE 
    behavior IN ('Curiosity & Critical Thinking')
    AND score IN (1, 2, 3)
GROUP BY 
    submissiondate, score