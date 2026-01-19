{{ config(
  materialized='table',
) }}

WITH base AS (
    SELECT
        *,
        CASE 
            WHEN subindicator IN ('cc1', 'cc2', 'cc3', 'cc4', 'cc5') THEN score
            ELSE NULL
        END AS filtered_score
    FROM {{ ref('classroom_surveys_normalized') }}
    WHERE role_coaching = 'elm' and behavior = 'Intentional Teaching'
)

SELECT
    region,
    program,
    behavior,
    submissiondate,
    score,
    "KEY",
    forms,
    country,
    sub_region,
    role_coaching,
    plname,
    education_level,
    observation_term,
    (SUM(filtered_score))::FLOAT / COUNT(filtered_score) AS ratio
FROM base
GROUP BY
    region, behavior, submissiondate, "KEY", forms, country, sub_region, role_coaching, score, program, plname, education_level,
    observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL OR sub_region IS NOT NULL
