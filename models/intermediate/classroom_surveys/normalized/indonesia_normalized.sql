{{ config(
    materialized='table',
) }}

-- UNION ALL of SurveyCTO Indonesia data and Kobo Indonesia data
-- SurveyCTO has 99 columns, Kobo has 131 columns
-- Creating unified column list: all columns from both tables in consistent order
-- Columns only in one table will be NULL in the other

WITH surveycto_data AS (
    SELECT
        -- Common columns (in order)
        c1, c2, c3, e1, e2,
        -- n1-n4 exist in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS n1,
        CAST(NULL AS varchar) AS n2,
        CAST(NULL AS varchar) AS n3,
        CAST(NULL AS varchar) AS n4,
        s1, s2, s3, s4,
        -- _id exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS _id,
        "KEY" AS "KEY",  -- SurveyCTO already has KEY column (keep as-is)
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
        caseid,  -- exists in SurveyCTO but not Kobo
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
        formdef_id,  -- exists in SurveyCTO but not Kobo
        -- device_det exists in Kobo but not SurveyCTO
        CAST(NULL AS varchar) AS device_det,
        "instanceID",  -- case-sensitive in SurveyCTO
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
        observer_role, role_coaching, "SubmissionDate",  -- case-sensitive in SurveyCTO
        coachee_gender, devicephonenum,
        review_quality,  -- exists in SurveyCTO but not Kobo
        teacher_gender, teacher_others,
        formdef_version,  -- exists in SurveyCTO but not Kobo
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
    FROM {{ source('source_classroom_surveys', 'indonesia') }}
),

kobo_data AS (
    SELECT
        -- Common columns (in same order as SurveyCTO)
        c1, c2, c3, e1, e2,
        n1, n2, n3, n4,
        s1, s2, s3, s4,
        _id,
        CONCAT('kobo_', _id) AS "KEY",  -- Prefix to ensure uniqueness (SurveyCTO has KEY, Kobo has _id)
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
)

SELECT * FROM surveycto_data
UNION ALL
SELECT * FROM kobo_data
