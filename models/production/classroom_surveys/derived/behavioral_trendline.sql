{{ config(
  materialized='table',
  schema=generate_schema_name('prod', this)
) }}

SELECT  region,
        DATE_TRUNC('month', submissiondate) AS submissiondate,
        "KEY",
        score,
        forms,
        program,
        sub_region,
        country,
        plname,
        education_level,
        observation_term,
        sum(CAST(case
                    when score=3 then 1
                    else 0
                end AS FLOAT)) / CAST(count(1) AS FLOAT) AS score_most
FROM {{ ref('classroom_surveys_normalized') }}
GROUP BY DATE_TRUNC('month', submissiondate), "KEY", score, forms, region, sub_region, country, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL