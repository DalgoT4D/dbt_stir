{{ config(
    materialized='table'
) }}

WITH survey_data AS (
    SELECT
        behavior,
        region,
        program,
        forms,
        sub_region,
        submissiondate,
        country,
        plname,
        education_level,
        observation_term,
        CASE
            WHEN (behavior = 'Safety') AND (score IN (1)) THEN 'Safety - A Few'
            WHEN (behavior = 'Safety') AND (score IN (2)) THEN 'Safety - About Half'
            WHEN (behavior = 'Safety') AND (score IN (3)) THEN 'Safety - Most'
            WHEN (behavior = 'Self Esteem') AND (score IN (1)) THEN 'Self Esteem - A Few'
            WHEN (behavior = 'Self Esteem') AND (score IN (2)) THEN 'Self Esteem - About Half'
            WHEN (behavior = 'Self Esteem') AND (score IN (3)) THEN 'Self Esteem - Most'
            WHEN (behavior = 'Engagement') AND (score IN (1)) THEN 'Engagement - A Few'
            WHEN (behavior = 'Engagement') AND (score IN (2)) THEN 'Engagement - About Half'
            WHEN (behavior = 'Engagement') AND (score IN (3)) THEN 'Engagement - Most'
            WHEN (behavior = 'Curiosity & Critical Thinking') AND (score IN (1)) THEN 'C&CT - A Few'
            WHEN (behavior = 'Curiosity & Critical Thinking') AND (score IN (2)) THEN 'C&CT - About Half'
            WHEN (behavior = 'Curiosity & Critical Thinking') AND (score IN (3)) THEN 'C&CT - Most'
            ELSE 'Other'
        END AS score_category,
        score,
        "KEY"
    FROM {{ ref('classroom_surveys_normalized') }}
    WHERE score IN (1, 2, 3)
        AND behavior IN ('Safety', 'Self Esteem', 'Engagement', 'Curiosity & Critical Thinking')
)

SELECT
    behavior,
    score_category,
    region,
    sub_region,
    country,
    submissiondate,
    "KEY",
    forms,
    program,
    plname, 
    education_level,
    observation_term,
    COUNT("KEY") AS count_keys,
    SUM(CAST(CASE WHEN score = 3 THEN 1 ELSE 0 END AS FLOAT)) / CAST(COUNT(1) AS FLOAT) AS score_most
FROM survey_data
GROUP BY behavior, region, submissiondate, "KEY", score, score_category, sub_region, country, forms, program, plname, education_level, observation_term
HAVING region IS NOT NULL AND sub_region IS NOT NULL