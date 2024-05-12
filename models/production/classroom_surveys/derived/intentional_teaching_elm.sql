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
    WHERE role_coaching = 'elm' and behavior = 'Intentional Teaching'
)

SELECT
    region,
    behavior,
    submissiondate,
    score,
    "KEY",
    forms,
    country,
    sub_region,
    role_coaching,
    (SUM(filtered_score))::FLOAT / COUNT(filtered_score) AS ratio
FROM base
GROUP BY
    region, behavior, submissiondate, "KEY", forms, country, sub_region, role_coaching, score
HAVING region IS NOT NULL AND sub_region IS NOT NULL OR sub_region IS NOT NULL