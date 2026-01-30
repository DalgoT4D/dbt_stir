{{ config(
    materialized='table',
    schema=generate_schema_name('prod_intermediate', this)
) }}

-- UNION ALL of SurveyCTO Uganda data and Kobo Uganda data
-- Uganda_SCTO schema: KEY and SubmissionDate from source (like TN/Karnataka); no _id, n*, _tags, cro13aiii_* variants
-- Kobo has more columns; SurveyCTO-only columns (caseid, instanceID, etc.) stay; Kobo-only get NULL in SurveyCTO
--
-- IMPORTANT: This model deduplicates data based on KEY column.
-- KEY and SubmissionDate are included from source (aligned with Tamil Nadu/Karnataka pattern).

WITH surveycto_data_raw AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY COALESCE("KEY", deviceid, starttime, date)) AS row_num
    FROM {{ source('source_classroom_surveys', 'uganda') }}
),
surveycto_data AS (
    SELECT
        c1, c2, c3, e1, e2,
        CAST(NULL AS varchar) AS n1,
        CAST(NULL AS varchar) AS n2,
        CAST(NULL AS varchar) AS n3,
        CAST(NULL AS varchar) AS n4,
        s1, s2, s3, s4,
        CAST(NULL AS varchar) AS _id,  -- SurveyCTO has KEY, not _id
        COALESCE(
            CASE WHEN "KEY" IS NOT NULL AND btrim("KEY") != '' THEN CONCAT('scto_', "KEY") ELSE NULL END,
            CONCAT('scto_', CAST(row_num AS varchar))
        ) AS "KEY",  -- KEY from source, prefixed for uniqueness (like TN/Karnataka include KEY)
        c1a, c2a,
        cc1, cc2, cc3, cc4, cc5,
        se1, se2, se3, se4, se5,
        cro1, cro2, cro3, cro4, cro5, cro7, cro8, cro9,
        date,
        CAST(NULL AS varchar) AS _tags,
        CAST(NULL AS varchar) AS _uuid,
        cro10, cro11, cro12, cro7a, cro8a,
        CAST(NULL AS varchar) AS _index,
        CAST(NULL AS varchar) AS _notes,
        cro13a, cro13b, cro13c,
        CAST(NULL AS varchar) AS cro13ai,
        CAST(NULL AS varchar) AS cro13aiv,
        CAST(NULL AS varchar) AS n_seca,
        CAST(NULL AS varchar) AS n_secc,
        plname,
        CAST(NULL AS varchar) AS _status,
        endtime, meeting,
        CAST(NULL AS varchar) AS n1_secd,
        CAST(NULL AS varchar) AS n1_sece,
        CAST(NULL AS varchar) AS n2_sece,
        CAST(NULL AS varchar) AS n3_sece,
        CAST(NULL AS varchar) AS n4_sece,
        CAST(NULL AS varchar) AS n5_sece,
        program, remarks,
        "Location",  -- SurveyCTO: lat long string (e.g. -0.3585426 32.6188692 1204.7000732421875 15.285); retained in uganda_normalized
        deviceid, duration, expected, username,
        cro13aiii, starttime,
        CAST(NULL AS varchar) AS __version__,
        device_info,
        caseid,
        formdef_id,
        "instanceID",
        review_quality,
        formdef_version,
        CAST(NULL AS varchar) AS "@",
        malepresent, coach_gender, forms_uganda, si_districts,
        CAST(NULL AS varchar) AS _submitted_by,
        date_coaching, district_teso, femalepresent,
        observer_role, role_coaching,
        "SubmissionDate",  -- SubmissionDate from source (like TN/Karnataka)
        coachee_gender, devicephonenum,
        district_lango, district_mbale, teacher_gender, teacher_others,
        district_acholi, district_ankole, district_busoga, district_kigezi,
        district_masaka, education_level, location_uganda, observer_gender,
        observer_others,
        CAST(NULL AS varchar) AS _submission_time,
        district_bunyoro, district_central,
        facilitator_role, observation_term, remarks_coaching, district_karamoja,
        district_rwenzori, district_westnile, duration_coaching, name_of_the_coach,
        remarks_classroom,
        CAST(NULL AS varchar) AS _Location_altitude,
        CAST(NULL AS varchar) AS _Location_latitude,
        CAST(NULL AS varchar) AS _validation_status,
        facilitator_gender, facilitator_others,
        CAST(NULL AS varchar) AS _Location_longitude,
        CAST(NULL AS varchar) AS _Location_precision,
        name_of_the_coachee, coach_gender_specify,
        CAST(NULL AS varchar) AS cro13aiii_dual_coding,
        CAST(NULL AS varchar) AS cro13aiii_exit_ticket,
        CAST(NULL AS varchar) AS cro13aiii_growth_mindset,
        CAST(NULL AS varchar) AS cro13aiii_spaced_practice,
        CAST(NULL AS varchar) AS cro13aiii_worked_examples,
        facilitator_role_coaching,
        CAST(NULL AS varchar) AS cro13aiii_the_four_corners,
        CAST(NULL AS varchar) AS cro13aiii_covid_19_strategies,
        CAST(NULL AS varchar) AS cro13aiii_retrieval_practices,
        CAST(NULL AS varchar) AS cro13aiii_safety_mapping_walk,
        CAST(NULL AS varchar) AS cro13aiii_breaking_down_learning,
        CAST(NULL AS varchar) AS cro13aiii_elaborative_questioning,
        CAST(NULL AS varchar) AS cro13aiii_focussed_lesson_objective,
        CAST(NULL AS varchar) AS cro13aiii_safe_learning_Environment,
        CAST(NULL AS varchar) AS cro13aiii_socio_emotional_wellbeing,
        CAST(NULL AS varchar) AS cro13aiii_asking_effective_questions,
        CAST(NULL AS varchar) AS cro13aiii_giving___receiving_feedback,
        CAST(NULL AS varchar) AS cro13aiii_physical_learning_environment,
        CAST(NULL AS varchar) AS cro13aiii_emotional_learning_environment,
        CAST(NULL AS varchar) AS cro13aiii_building_positive_relationships,
        CAST(NULL AS varchar) AS cro13aiii_formative_assessment_strategies,
        coachee_gender_specify,
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta
    FROM surveycto_data_raw
),

kobo_data AS (
    SELECT
        c1, c2, c3, e1, e2, n1, n2, n3, n4,
        s1, s2, s3, s4, 
        _id AS "KEY",  -- Use _id directly for KEY column
        _id, c1a, c2a,
        cc1, cc2, cc3, cc4, cc5,
        se1, se2, se3, se4, se5,
        cro1, cro2, cro3, cro4, cro5, cro7, cro8, cro9,
        date, _tags, _uuid,
        cro10, cro11, cro12, cro7a, cro8a,
        _index, _notes, cro13a, cro13b, cro13c,
        -- cro13ai, cro13aiv don't exist in Uganda (only in some other countries)
        CAST(NULL AS varchar) AS cro13ai,
        CAST(NULL AS varchar) AS cro13aiv,
        n_seca, n_secc, plname, _status,
        endtime, meeting,
        n1_secd, n1_sece, n2_sece, n3_sece, n4_sece, n5_sece,
        program, remarks,
        CAST(NULL AS varchar) AS "Location",  -- SurveyCTO has lat/long string (e.g. -0.3585426 32.6188692 ...); Kobo may not have it, retain column with NULL for Kobo
        deviceid, duration, expected, username,
        cro13aiii, starttime, __version__, device_info,
        -- Missing columns in Kobo (exist in SurveyCTO)
        CAST(NULL AS varchar) AS caseid,  -- exists in SurveyCTO but not Kobo
        CAST(NULL AS varchar) AS formdef_id,  -- exists in SurveyCTO but not Kobo
        CAST(NULL AS varchar) AS "instanceID",  -- exists in SurveyCTO but not Kobo
        CAST(NULL AS varchar) AS review_quality,  -- exists in SurveyCTO but not Kobo
        CAST(NULL AS varchar) AS formdef_version,  -- exists in SurveyCTO but not Kobo
        CAST(NULL AS varchar) AS "@",  -- exists in SurveyCTO but not Kobo
        malepresent, coach_gender, forms_uganda, si_districts,
        _submitted_by, date_coaching, district_teso, femalepresent,
        observer_role, role_coaching,
        CAST(NULL AS varchar) AS "SubmissionDate",  -- exists in SurveyCTO but not Kobo
        coachee_gender, devicephonenum,
        district_lango, district_mbale, teacher_gender, teacher_others,
        district_acholi, district_ankole, district_busoga, district_kigezi,
        district_masaka, education_level, location_uganda, observer_gender,
        observer_others, _submission_time, district_bunyoro, district_central,
        facilitator_role, observation_term, remarks_coaching, district_karamoja,
        district_rwenzori, district_westnile, duration_coaching, name_of_the_coach,
        remarks_classroom,
        CAST(NULL AS varchar) AS _Location_altitude,
        CAST(NULL AS varchar) AS _Location_latitude,
        CAST(NULL AS varchar) AS _validation_status,
        facilitator_gender, facilitator_others,
        CAST(NULL AS varchar) AS _Location_longitude,
        CAST(NULL AS varchar) AS _Location_precision,
        name_of_the_coachee, coach_gender_specify,
        -- cro13aiii variants now use underscores (not slashes) in Kobo
        cro13aiii_dual_coding,
        cro13aiii_exit_ticket,
        cro13aiii_growth_mindset,
        cro13aiii_spaced_practice,
        cro13aiii_worked_examples,
        facilitator_role_coaching,
        cro13aiii_the_four_corners,
        cro13aiii_covid_19_strategies,
        cro13aiii_retrieval_practices,
        cro13aiii_safety_mapping_walk,
        cro13aiii_breaking_down_learning,
        cro13aiii_elaborative_questioning,
        cro13aiii_focussed_lesson_objective,
        "cro13aiii_safe_learning_Environment",  -- quoted: capital E in source
        cro13aiii_socio_emotional_wellbeing,
        cro13aiii_asking_effective_questions,
        cro13aiii_giving___receiving_feedback,
        cro13aiii_physical_learning_environment,
        cro13aiii_emotional_learning_environment,
        cro13aiii_building_positive_relationships,
        cro13aiii_formative_assessment_strategies,
        coachee_gender_specify,
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta
    FROM {{ source('source_classroom_surveys', 'uganda_kobo') }}
),

combined_data AS (
    SELECT * FROM surveycto_data
    UNION ALL
    SELECT * FROM kobo_data
),

-- Deduplicate based on KEY column, keeping the latest record per KEY
-- Deduplicate based on KEY column in dev_intermediate database
-- Filter out NULL or blank KEY values to ensure data quality
-- This ensures no duplicates in the intermediate schema tables
deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY "KEY"
                ORDER BY 
                    COALESCE(
                        -- Handle _submission_time (Kobo Excel serial date or timestamp string)
                        CASE
                            WHEN btrim(_submission_time) ~ '^[0-9]+(\.[0-9]+)?$'
                                THEN (timestamp '1899-12-30' + (btrim(_submission_time)::double precision * interval '1 day'))
                            WHEN _submission_time IS NOT NULL AND _submission_time ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
                                THEN to_timestamp(btrim(_submission_time), 'DD/MM/YYYY HH24:MI:SS')
                            WHEN _submission_time IS NOT NULL AND _submission_time ~ '^\d{2}/\d{2}/\d{4}'
                                THEN to_timestamp(btrim(_submission_time), 'DD/MM/YYYY')
                            WHEN _submission_time IS NOT NULL
                                THEN btrim(_submission_time)::timestamp
                            ELSE NULL
                        END,
                        -- Handle starttime (Excel serial date or timestamp string)
                        CASE
                            WHEN btrim(starttime) ~ '^[0-9]+(\.[0-9]+)?$'
                                THEN (timestamp '1899-12-30' + (btrim(starttime)::double precision * interval '1 day'))
                            WHEN starttime IS NOT NULL AND starttime ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
                                THEN to_timestamp(btrim(starttime), 'DD/MM/YYYY HH24:MI:SS')
                            WHEN starttime IS NOT NULL AND starttime ~ '^\d{2}/\d{2}/\d{4}'
                                THEN to_timestamp(btrim(starttime), 'DD/MM/YYYY')
                            WHEN starttime IS NOT NULL
                                THEN starttime::timestamp
                            ELSE NULL
                        END,
                        -- Handle date field
                        CASE
                            WHEN date IS NOT NULL AND date ~ '^\d{2}/\d{2}/\d{4}'
                                THEN to_timestamp(date, 'DD/MM/YYYY')::timestamp
                            WHEN date IS NOT NULL
                                THEN to_date(date, 'YYYY-MM-DD')::timestamp
                            ELSE NULL
                        END
                    ) DESC NULLS LAST
            ) AS rn
        FROM combined_data
        WHERE "KEY" IS NOT NULL 
          AND btrim("KEY") != ''  -- Filter out NULL and blank KEY values
    ) ranked
    WHERE rn = 1
)

SELECT * FROM deduplicated
