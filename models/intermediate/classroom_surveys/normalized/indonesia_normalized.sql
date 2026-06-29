{{ config(
    materialized='table',
    schema=generate_schema_name('prod_intermediate', this)
) }}

-- UNION ALL of SurveyCTO Indonesia data and Kobo Indonesia data
-- Indonesia_Kobo: map prefixed sheet columns → canonical behaviour names in kobo_data (this is the intentional survey redesign).
-- Indonesia_SCTO: if staging still exposes legacy names (c1, "KEY"), surveycto_data could select them directly; many warehouses
-- normalize to snake_case/underscores (c_1, key, submission_date). Alias those here to the same canonical names as Kobo so UNION
-- and downstream models stay stable — values are unchanged, only column renames in the physical table.
--
-- IMPORTANT: This model deduplicates data based on KEY column.

WITH surveycto_data_raw AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY COALESCE(key, deviceid, starttime, date)) AS row_num
    FROM {{ source('source_classroom_surveys', 'indonesia') }}
),
surveycto_data AS (
    SELECT
        s.c_1 AS c1,
        s.c_2 AS c2,
        s.c_3 AS c3,
        s.e_1 AS e1,
        s.e_2 AS e2,
        CAST(NULL AS varchar) AS n1,
        CAST(NULL AS varchar) AS n2,
        CAST(NULL AS varchar) AS n3,
        CAST(NULL AS varchar) AS n4,
        s.s_1 AS s1,
        s.s_2 AS s2,
        s.s_3 AS s3,
        s.s_4 AS s4,
        CAST(NULL AS varchar) AS _id,
        COALESCE(
            CASE WHEN s.key IS NOT NULL AND btrim(s.key::text) != '' THEN CONCAT('scto_', s.key::text) ELSE NULL END,
            CONCAT('scto_', CAST(s.row_num AS varchar))
        ) AS "KEY",
        s.c_1_a AS c1a,
        s.c_2_a AS c2a,
        s.cc_1 AS cc1,
        s.cc_2 AS cc2,
        s.cc_3 AS cc3,
        s.cc_4 AS cc4,
        s.cc_5 AS cc5,
        CAST(NULL AS varchar) AS "end",
        s.gc_1 AS gc1,
        s.gc_2 AS gc2,
        s.gc_3 AS gc3,
        s.gc_4 AS gc4,
        s.gc_5 AS gc5,
        s.se_1 AS se1,
        s.se_2 AS se2,
        s.se_3 AS se3,
        s.se_4 AS se4,
        s.se_5 AS se5,
        s.cro_1 AS cro1,
        s.cro_2 AS cro2,
        s.cro_3 AS cro3,
        s.cro_4 AS cro4,
        s.cro_5 AS cro5,
        s.cro_7 AS cro7,
        s.cro_8 AS cro8,
        s.cro_9 AS cro9,
        s.date,
        s.ge_1,
        s.ge_2,
        s.ge_3,
        s.ge_4,
        s.ge_5,
        CAST(NULL AS varchar) AS _tags,
        CAST(NULL AS varchar) AS _uuid,
        s.cro_10 AS cro10,
        s.cro_11 AS cro11,
        s.cro_12 AS cro12,
        s.cro_7_a AS cro7a,
        s.cro_8_a AS cro8a,
        CAST(NULL AS varchar) AS start,
        CAST(NULL AS varchar) AS _index,
        CAST(NULL AS varchar) AS _notes,
        s.caseid,
        s.cro_13_a AS cro13a,
        s.cro_13_b AS cro13b,
        s.cro_13_c AS cro13c,
        CAST(NULL AS varchar) AS cro13ai,
        CAST(NULL AS varchar) AS cro13aiii,
        CAST(NULL AS varchar) AS cro13aiv,
        CAST(NULL AS varchar) AS n_seca,
        CAST(NULL AS varchar) AS n_secc,
        CAST(NULL AS varchar) AS _status,
        s.cro_13_av AS cro13av,
        s.endtime,
        s.meeting,
        CAST(NULL AS varchar) AS n1_secd,
        CAST(NULL AS varchar) AS n1_sece,
        CAST(NULL AS varchar) AS n2_sece,
        CAST(NULL AS varchar) AS n2_secf,
        CAST(NULL AS varchar) AS n3_sece,
        CAST(NULL AS varchar) AS n4_sece,
        CAST(NULL AS varchar) AS n5_sece,
        s.remarks,
        CAST(NULL AS varchar) AS date_cro,
        s.deviceid,
        s.duration,
        s.expected,
        s.username,
        s.programme,
        s.starttime,
        s.formdef_id,
        CAST(NULL AS varchar) AS device_det,
        s.instance_id::varchar AS "instanceID",
        CAST(NULL AS varchar) AS __version__,
        s.device_info,
        s.malepresent,
        CAST(NULL AS varchar) AS phonenumber,
        s.type_school,
        s.coach_gender,
        CAST(NULL AS varchar) AS _submitted_by,
        s.date_coaching,
        s.femalepresent,
        CAST(NULL AS varchar) AS interview_dur,
        s.observer_role,
        s.role_coaching,
        s.submission_date::varchar AS "SubmissionDate",
        s.coachee_gender,
        s.devicephonenum,
        s.review_quality,
        s.teacher_gender,
        s.teacher_others,
        s.formdef_version,
        s.forms_indonesia,
        s.observer_gender,
        s.observer_others,
        CAST(NULL AS varchar) AS _submission_time,
        s.facilitator_role,
        s.meeting_coaching,
        s.observation_term,
        s.remarks_coaching,
        s.duration_coaching,
        s.name_of_the_coach,
        s.remarks_classroom,
        CAST(NULL AS varchar) AS _validation_status,
        s.district_indonesia,
        s.facilitator_gender,
        s.facilitator_others,
        s.location_indonesia,
        s.name_of_the_coachee,
        s.coach_gender_specify,
        s.coachee_gender_specify,
        s.remarks_group_coaching,
        CAST(NULL AS varchar) AS cro13a_digital_learning,
        CAST(NULL AS varchar) AS cro13av_digital_learning,
        s.facilitator_role_coaching,
        CAST(NULL AS varchar) AS cro13a_settlers___stirrers,
        CAST(NULL AS varchar) AS cro13a_pairwork___groupwork,
        CAST(NULL AS varchar) AS cro13av_settlers___stirrers,
        CAST(NULL AS varchar) AS cro13av_pairwork___groupwork,
        CAST(NULL AS varchar) AS cro13a_collab___coop_learning,
        CAST(NULL AS varchar) AS cro13av_collab___coop_learning,
        CAST(NULL AS varchar) AS cro13a_differentiated_instruction,
        CAST(NULL AS varchar) AS cro13av_differentiated_instruction,
        s._airbyte_raw_id,
        s._airbyte_extracted_at,
        s._airbyte_meta
    FROM surveycto_data_raw AS s
),

kobo_data AS (
    SELECT
        /* Same column order and names as surveycto_data; map prefixed sheet columns → canonical fields */
        k.b_secb_c_1 AS c1,
        k.b_secb_c_2 AS c2,
        k.b_secb_c_3 AS c3,
        k.b_secb_e_1 AS e1,
        k.b_secb_e_2 AS e2,
        k.b_secb_n_1 AS n1,
        k.b_secb_n_2 AS n2,
        k.b_secb_n_3 AS n3,
        k.b_secb_n_4 AS n4,
        k.b_secb_s_1 AS s1,
        k.b_secb_s_2 AS s2,
        k.b_secb_s_3 AS s3,
        k.b_secb_s_4 AS s4,
        k._id::varchar AS _id,
        k._id::varchar AS "KEY",
        k.b_secb_c_1_a AS c1a,
        k.b_secb_c_2_a AS c2a,
        k.g_secd_cc_1 AS cc1,
        k.g_secd_cc_2 AS cc2,
        k.g_secd_cc_3 AS cc3,
        k.g_secd_cc_4 AS cc4,
        k.g_secd_cc_5 AS cc5,
        k."end",
        k.g_secf_gc_1 AS gc1,
        k.g_secf_gc_2 AS gc2,
        k.g_secf_gc_3 AS gc3,
        k.g_secf_gc_4 AS gc4,
        k.g_secf_gc_5 AS gc5,
        k.b_secb_se_1 AS se1,
        k.b_secb_se_2 AS se2,
        k.b_secb_se_3 AS se3,
        k.b_secb_se_4 AS se4,
        k.b_secb_se_5 AS se5,
        k.g_sece_cro_1 AS cro1,
        k.g_sece_cro_2 AS cro2,
        k.g_sece_cro_3 AS cro3,
        k.g_sece_cro_4 AS cro4,
        k.g_sece_cro_5 AS cro5,
        k.g_sece_cro_7 AS cro7,
        k.g_sece_cro_8 AS cro8,
        k.g_sece_cro_9 AS cro9,
        k.a_seca_date AS date,
        k.ge_1, k.ge_2, k.ge_3, k.ge_4, k.ge_5,
        k._tags,
        k._uuid,
        k.g_sece_cro_10 AS cro10,
        k.g_sece_cro_11 AS cro11,
        k.g_sece_cro_12 AS cro12,
        k.g_sece_cro_7_a AS cro7a,
        k.g_sece_cro_8_a AS cro8a,
        k.start,
        k._index,
        k._notes,
        CAST(NULL AS varchar) AS caseid,
        k.g_sece_cro_13_a AS cro13a,
        k.g_sece_cro_13_b AS cro13b,
        k.g_sece_cro_13_c AS cro13c,
        CAST(NULL AS varchar) AS cro13ai,
        CAST(NULL AS varchar) AS cro13aiii,
        CAST(NULL AS varchar) AS cro13aiv,
        k.a_seca_n_seca AS n_seca,
        k.g_secc_n_secc AS n_secc,
        k._status,
        k.g_sece_cro_13_av AS cro13av,
        k.endtime,
        k.meeting,
        k.g_secd_n_1_secd AS n1_secd,
        k.g_sece_n_1_sece AS n1_sece,
        k.g_sece_n_2_sece AS n2_sece,
        k.g_secf_n_2_secf AS n2_secf,
        k.g_sece_n_3_sece AS n3_sece,
        k.g_sece_n_4_sece AS n4_sece,
        k.g_sece_n_5_sece AS n5_sece,
        k.remarks,
        k.date_cro,
        k.deviceid,
        k.duration,
        k.a_seca_expected AS expected,
        k.username,
        k.programme,
        k.starttime,
        CAST(NULL AS varchar) AS formdef_id,
        k.device_det,
        CAST(NULL AS varchar) AS "instanceID",
        k._version_::varchar AS "__version__",
        k.device_info,
        k.a_seca_malepresent AS malepresent,
        k.phonenumber,
        k.type_school,
        k.coach_gender,
        k._submitted_by,
        k.g_secc_date_coaching AS date_coaching,
        k.a_seca_femalepresent AS femalepresent,
        k.interview_dur,
        k.observer_role,
        k.g_secc_role_coaching AS role_coaching,
        CAST(NULL AS varchar) AS "SubmissionDate",
        k.coachee_gender,
        k.devicephonenum,
        CAST(NULL AS varchar) AS review_quality,
        k.teacher_gender,
        k.teacher_others,
        CAST(NULL AS varchar) AS formdef_version,
        k.forms_indonesia,
        k.observer_gender,
        k.observer_others,
        k._submission_time,
        k.facilitator_role,
        k.g_secc_meeting_coaching AS meeting_coaching,
        k.observation_term,
        k.g_secd_remarks_coaching AS remarks_coaching,
        k.g_secc_duration_coaching AS duration_coaching,
        k.g_secc_name_of_the_coach AS name_of_the_coach,
        k.g_sece_remarks_classroom AS remarks_classroom,
        k._validation_status,
        k.district_indonesia,
        k.facilitator_gender,
        k.facilitator_others,
        k.location_indonesia,
        k.g_secc_name_of_the_coachee AS name_of_the_coachee,
        k.coach_gender_specify,
        k.coachee_gender_specify,
        k.g_secf_remarks_group_coaching AS remarks_group_coaching,
        k.g_sece_cro_13_a_digital_learning AS cro13a_digital_learning,
        k.g_sece_cro_13_av_digital_learning AS cro13av_digital_learning,
        k.g_secc_facilitator_role_coaching AS facilitator_role_coaching,
        k.g_sece_cro_13_a_settlers_stirrers AS cro13a_settlers___stirrers,
        k.g_sece_cro_13_a_pairwork_groupwork AS cro13a_pairwork___groupwork,
        k.g_sece_cro_13_av_settlers_stirrers AS cro13av_settlers___stirrers,
        k.g_sece_cro_13_av_pairwork_groupwork AS cro13av_pairwork___groupwork,
        k.g_sece_cro_13_a_collab_coop_learning AS cro13a_collab___coop_learning,
        k.g_sece_cro_13_av_collab_coop_learning AS cro13av_collab___coop_learning,
        k.g_sece_cro_13_a_differentiated_instruction AS cro13a_differentiated_instruction,
        k.g_sece_cro_13_av_differentiated_instruction AS cro13av_differentiated_instruction,
        k._airbyte_raw_id,
        k._airbyte_extracted_at,
        k._airbyte_meta
    FROM {{ source('source_classroom_surveys', 'indonesia_kobo') }} AS k
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
