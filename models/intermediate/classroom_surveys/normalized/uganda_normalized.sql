{{ config(
    materialized='table',
) }}

-- UNION ALL of SurveyCTO Uganda data and Kobo Uganda data
-- Both tables have identical structure (141 columns in same order)
-- Using explicit column list to ensure type compatibility and maintainability

WITH surveycto_data AS (
    SELECT
        c1, c2, c3, e1, e2, n1, n2, n3, n4,
        s1, s2, s3, s4, 
        CONCAT('scto_', _id) AS "KEY",  -- Prefix to ensure uniqueness across sources
        _id, c1a, c2a,
        cc1, cc2, cc3, cc4, cc5,
        se1, se2, se3, se4, se5,
        cro1, cro2, cro3, cro4, cro5, cro7, cro8, cro9,
        date, _tags, _uuid,
        cro10, cro11, cro12, cro7a, cro8a,
        _index, _notes, cro13a, cro13b, cro13c,
        n_seca, n_secc, plname, _status,
        endtime, meeting,
        n1_secd, n1_sece, n2_sece, n3_sece, n4_sece, n5_sece,
        program, remarks, "Location",
        deviceid, duration, expected, username,
        cro13aiii, starttime, __version__, device_info,
        malepresent, coach_gender, forms_uganda, si_districts,
        _submitted_by, date_coaching, district_teso, femalepresent,
        observer_role, role_coaching, coachee_gender, devicephonenum,
        district_lango, district_mbale, teacher_gender, teacher_others,
        district_acholi, district_ankole, district_busoga, district_kigezi,
        district_masaka, education_level, location_uganda, observer_gender,
        observer_others, _submission_time, district_bunyoro, district_central,
        facilitator_role, observation_term, remarks_coaching, district_karamoja,
        district_rwenzori, district_westnile, duration_coaching, name_of_the_coach,
        remarks_classroom, "_Location_altitude", "_Location_latitude", "_validation_status",
        facilitator_gender, facilitator_others, "_Location_longitude", "_Location_precision",
        name_of_the_coachee, coach_gender_specify,
        cro13aiii_dual_coding, cro13aiii_exit_ticket, coachee_gender_specify,
        cro13aiii_growth_mindset, cro13aiii_spaced_practice, cro13aiii_worked_examples,
        facilitator_role_coaching, cro13aiii_the_four_corners, cro13aiii_covid_19_strategies,
        cro13aiii_retrieval_practices, cro13aiii_safety_mapping_walk, cro13aiii_breaking_down_learning,
        cro13aiii_elaborative_questioning, cro13aiii_focussed_lesson_objective,
        "cro13aiii_safe_learning_Environment", cro13aiii_socio_emotional_wellbeing,
        cro13aiii_asking_effective_questions, cro13aiii_giving___receiving_feedback,
        cro13aiii_physical_learning_environment, cro13aiii_emotional_learning_environment,
        cro13aiii_building_positive_relationships, cro13aiii_formative_assessment_strategies,
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta
    FROM {{ source('source_classroom_surveys', 'uganda') }}
),

kobo_data AS (
    SELECT
        c1, c2, c3, e1, e2, n1, n2, n3, n4,
        s1, s2, s3, s4, 
        CONCAT('kobo_', _id) AS "KEY",  -- Prefix to ensure uniqueness across sources
        _id, c1a, c2a,
        cc1, cc2, cc3, cc4, cc5,
        se1, se2, se3, se4, se5,
        cro1, cro2, cro3, cro4, cro5, cro7, cro8, cro9,
        date, _tags, _uuid,
        cro10, cro11, cro12, cro7a, cro8a,
        _index, _notes, cro13a, cro13b, cro13c,
        n_seca, n_secc, plname, _status,
        endtime, meeting,
        n1_secd, n1_sece, n2_sece, n3_sece, n4_sece, n5_sece,
        program, remarks, "Location",
        deviceid, duration, expected, username,
        cro13aiii, starttime, __version__, device_info,
        malepresent, coach_gender, forms_uganda, si_districts,
        _submitted_by, date_coaching, district_teso, femalepresent,
        observer_role, role_coaching, coachee_gender, devicephonenum,
        district_lango, district_mbale, teacher_gender, teacher_others,
        district_acholi, district_ankole, district_busoga, district_kigezi,
        district_masaka, education_level, location_uganda, observer_gender,
        observer_others, _submission_time, district_bunyoro, district_central,
        facilitator_role, observation_term, remarks_coaching, district_karamoja,
        district_rwenzori, district_westnile, duration_coaching, name_of_the_coach,
        remarks_classroom, "_Location_altitude", "_Location_latitude", "_validation_status",
        facilitator_gender, facilitator_others, "_Location_longitude", "_Location_precision",
        name_of_the_coachee, coach_gender_specify,
        cro13aiii_dual_coding, cro13aiii_exit_ticket, coachee_gender_specify,
        cro13aiii_growth_mindset, cro13aiii_spaced_practice, cro13aiii_worked_examples,
        facilitator_role_coaching, cro13aiii_the_four_corners, cro13aiii_covid_19_strategies,
        cro13aiii_retrieval_practices, cro13aiii_safety_mapping_walk, cro13aiii_breaking_down_learning,
        cro13aiii_elaborative_questioning, cro13aiii_focussed_lesson_objective,
        "cro13aiii_safe_learning_Environment", cro13aiii_socio_emotional_wellbeing,
        cro13aiii_asking_effective_questions, cro13aiii_giving___receiving_feedback,
        cro13aiii_physical_learning_environment, cro13aiii_emotional_learning_environment,
        cro13aiii_building_positive_relationships, cro13aiii_formative_assessment_strategies,
        _airbyte_raw_id, _airbyte_extracted_at, _airbyte_meta
    FROM {{ source('source_classroom_surveys', 'uganda_kobo') }}
)

SELECT * FROM surveycto_data
UNION ALL
SELECT * FROM kobo_data
