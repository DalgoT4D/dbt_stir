{{ config(
    materialized='table',
) }}

-- UNION ALL of SurveyCTO Indonesia data and Kobo Indonesia data
-- SurveyCTO has 99 columns, Kobo has 131 columns
-- Creating unified column list: all columns from both tables in consistent order
-- Columns only in one table will be NULL in the other
--
-- IMPORTANT: This model deduplicates data based on KEY column in the dev_intermediate database.
-- Downstream tables may have repeated KEY values (e.g., one KEY per subindicator, time period, etc.)
-- which is expected for data processing needs. Deduplication only happens here.

WITH surveycto_data_raw AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY COALESCE(_id, deviceid, starttime, date)) AS row_num
    FROM {{ source('source_classroom_surveys', 'indonesia') }}
),
surveycto_data AS (
    SELECT
        -- Common columns (in order)
        c1, c2, c3, e1, e2,
        -- n1-n4 exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS n1,
        CAST(NULL AS varchar) AS n2,
        CAST(NULL AS varchar) AS n3,
        CAST(NULL AS varchar) AS n4,
        s1, s2, s3, s4,
        _id,
        COALESCE(
            CASE WHEN _id IS NOT NULL AND btrim(_id) != '' THEN CONCAT('scto_', _id) ELSE NULL END,
            CONCAT('scto_', CAST(row_num AS varchar))
        ) AS "KEY",  -- Prefix SurveyCTO _id to ensure uniqueness, use row_num if _id is NULL
        c1a, c2a,
        cc1, cc2, cc3, cc4, cc5,
        -- "end" exists in Kobo but not SurveyCTO (quoted: reserved word)
        CAST(NULL AS varchar) AS "end",
        gc1, gc2, gc3, gc4, gc5,
        se1, se2, se3, se4, se5,
        cro1, cro2, cro3, cro4, cro5, cro7, cro8, cro9,
        date, ge_1, ge_2, ge_3, ge_4, ge_5,
        -- _tags, _uuid exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS _tags,
        CAST(NULL AS varchar) AS _uuid,
        cro10, cro11, cro12, cro7a, cro8a,
        -- start exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS start,
        -- _index, _notes exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS _index,
        CAST(NULL AS varchar) AS _notes,
        CAST(NULL AS varchar) AS caseid,  -- caseid doesn't exist in SurveyCTO Indonesia
        cro13a, cro13b, cro13c,
        -- n_seca, n_secc, _status exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS n_seca,
        CAST(NULL AS varchar) AS n_secc,
        CAST(NULL AS varchar) AS _status,
        cro13av,
        endtime, meeting,
        -- n1_secd, n1_sece, n2_sece, n2_secf, n3_sece, n4_sece, n5_sece exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS n1_secd,
        CAST(NULL AS varchar) AS n1_sece,
        CAST(NULL AS varchar) AS n2_sece,
        CAST(NULL AS varchar) AS n2_secf,
        CAST(NULL AS varchar) AS n3_sece,
        CAST(NULL AS varchar) AS n4_sece,
        CAST(NULL AS varchar) AS n5_sece,
        remarks,
        -- date_cro exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS date_cro,
        deviceid, duration, expected, username, programme,
        starttime,
        CAST(NULL AS varchar) AS formdef_id,  -- formdef_id doesn't exist in SurveyCTO Indonesia
        -- device_det exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS device_det,
        CAST(NULL AS varchar) AS "instanceID",  -- instanceID doesn't exist in SurveyCTO Indonesia
        -- __version__ exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS __version__,
        device_info,
        malepresent,
        -- phonenumber exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS phonenumber,
        type_school, coach_gender,
        -- _submitted_by exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS _submitted_by,
        date_coaching, femalepresent,
        -- interview_dur exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS interview_dur,
        observer_role, role_coaching, CAST(NULL AS varchar) AS "SubmissionDate",  -- SubmissionDate doesn't exist in SurveyCTO Indonesia
        coachee_gender, devicephonenum,
        CAST(NULL AS varchar) AS review_quality,  -- review_quality doesn't exist in SurveyCTO Indonesia
        teacher_gender, teacher_others,
        CAST(NULL AS varchar) AS formdef_version,  -- formdef_version doesn't exist in SurveyCTO Indonesia
        forms_indonesia,
        observer_gender, observer_others,
        -- _submission_time exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS _submission_time,
        facilitator_role, meeting_coaching, observation_term,
        remarks_coaching, duration_coaching, name_of_the_coach,
        remarks_classroom,
        -- _validation_status exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS _validation_status,
        district_indonesia, facilitator_gender, facilitator_others,
        location_indonesia, name_of_the_coachee,
        coach_gender_specify, coachee_gender_specify,
        remarks_group_coaching,
        -- cro13a_digital_learning, cro13av_digital_learning exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS cro13a_digital_learning,
        CAST(NULL AS varchar) AS cro13av_digital_learning,
        facilitator_role_coaching,
        -- cro13a_settlers___stirrers, cro13a_pairwork___groupwork exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS cro13a_settlers___stirrers,
        CAST(NULL AS varchar) AS cro13a_pairwork___groupwork,
        -- cro13av_settlers___stirrers, cro13av_pairwork___groupwork exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS cro13av_settlers___stirrers,
        CAST(NULL AS varchar) AS cro13av_pairwork___groupwork,
        -- cro13a_collab___coop_learning, cro13av_collab___coop_learning exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS cro13a_collab___coop_learning,
        CAST(NULL AS varchar) AS cro13av_collab___coop_learning,
        -- cro13a_differentiated_instruction, cro13av_differentiated_instruction exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS cro13a_differentiated_instruction,
        CAST(NULL AS varchar) AS cro13av_differentiated_instruction,
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta
    FROM surveycto_data_raw
),

kobo_data AS (
    SELECT
        -- Common columns (in same order as SurveyCTO)
        c1, c2, c3, e1, e2,
        n1, n2, n3, n4,
        s1, s2, s3, s4,
        _id,
        _id AS "KEY",  -- Use _id directly for KEY column
        c1a, c2a,
        cc1, cc2, cc3, cc4, cc5,
        "end",
        gc1, gc2, gc3, gc4, gc5,
        se1, se2, se3, se4, se5,
        cro1, cro2, cro3, cro4, cro5, cro7, cro8, cro9,
        date, ge_1, ge_2, ge_3, ge_4, ge_5,
        _tags, _uuid,
        cro10, cro11, cro12, cro7a, cro8a,
        start,
        _index, _notes,
        CAST(NULL AS varchar) AS caseid,  -- exists in SurveyCTO but not Kobo
        cro13a, cro13b, cro13c,
        n_seca, n_secc, _status,
        cro13av,
        endtime, meeting,
        n1_secd, n1_sece, n2_sece, n2_secf, n3_sece, n4_sece, n5_sece,
        remarks,
        date_cro,
        deviceid, duration, expected, username, programme,
        starttime,
        CAST(NULL AS varchar) AS formdef_id,  -- exists in SurveyCTO but not Kobo
        device_det,
        CAST(NULL AS varchar) AS "instanceID",  -- exists in SurveyCTO but not Kobo
        __version__,
        device_info,
        malepresent,
        phonenumber,
        type_school, coach_gender,
        _submitted_by,
        date_coaching, femalepresent,
        interview_dur,
        observer_role, role_coaching,
        CAST(NULL AS varchar) AS "SubmissionDate",  -- exists in SurveyCTO but not Kobo
        coachee_gender, devicephonenum,
        CAST(NULL AS varchar) AS review_quality,  -- exists in SurveyCTO but not Kobo
        teacher_gender, teacher_others,
        CAST(NULL AS varchar) AS formdef_version,  -- exists in SurveyCTO but not Kobo
        forms_indonesia,
        observer_gender, observer_others,
        _submission_time,
        facilitator_role, meeting_coaching, observation_term,
        remarks_coaching, duration_coaching, name_of_the_coach,
        remarks_classroom,
        _validation_status,
        district_indonesia, facilitator_gender, facilitator_others,
        location_indonesia, name_of_the_coachee,
        coach_gender_specify, coachee_gender_specify,
        remarks_group_coaching,
        cro13a_digital_learning, cro13av_digital_learning,
        facilitator_role_coaching,
        cro13a_settlers___stirrers, cro13a_pairwork___groupwork,
        cro13av_settlers___stirrers, cro13av_pairwork___groupwork,
        cro13a_collab___coop_learning, cro13av_collab___coop_learning,
        cro13a_differentiated_instruction, cro13av_differentiated_instruction,
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta
    FROM {{ source('source_classroom_surveys', 'indonesia_kobo') }}
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
