{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

WITH base AS (
    SELECT
        *,
        CASE 
            WHEN subindicator IN ('cc1', 'cc2', 'cc3', 'cc4', 'cc5') THEN score
            ELSE NULL
        END AS filtered_score
    FROM {{ ref('classroom_surveys_normalized') }}
    WHERE (role_coaching = 'dc' or role_coaching = 'asshu' or role_coaching = 'dpo') and behavior = 'Intentional Teaching'
)

SELECT
    region,
    program,
    behavior,
    submissiondate,
    score,
    country,
    "KEY",
    forms,
    sub_region,
    role_coaching,
    plname,
    education_level,
    observation_term,
    (SUM(filtered_score))::FLOAT / COUNT(filtered_score) AS ratio
FROM base
GROUP BY
    role_coaching, region, submissiondate, "KEY", forms, sub_region, score, behavior, country, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL OR sub_region IS NOT NULL
