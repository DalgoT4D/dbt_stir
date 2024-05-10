{{ config(
  materialized='table',
  schema=generate_schema_name('prod_intermediate', this)
) }}

with cte as (
  {{ flatten_json(
      model_name = source('source_classroom_surveys', 'ethiopia'),
      json_column = 'data'
  ) }}
)

SELECT 
    CAST(c1 AS bigint) AS c1,
    CAST(c2 AS bigint) AS c2,
    CAST(c3 AS bigint) AS c3,
    CAST(e1 AS bigint) AS e1,
    CAST(e2 AS bigint) AS e2,
    n1,
    n2,
    n3,
    n4,
    CAST(s1 AS bigint) AS s1,
    CAST(s2 AS bigint) AS s2,
    CAST(s3 AS bigint) AS s3,
    CAST(s4 AS bigint) AS s4,
    "KEY",
    c1a,
    c2a,
    CAST(cc1 AS bigint) AS cc1,
    CAST(cc2 AS bigint) AS cc2,
    CAST(cc3 AS bigint) AS cc3,
    CAST(cc4 AS bigint) AS cc4,
    CAST(cc5 AS bigint) AS cc5,
    CAST(se1 AS bigint) AS se1,
    CAST(se2 AS bigint) AS se2,
    CAST(se3 AS bigint) AS se3,
    CAST(se4 AS bigint) AS se4,
    CAST(se5 AS bigint) AS se5,
    CAST(ad_q AS bigint) AS ad_q,
    CAST(cro1 AS bigint) AS cro1,
    CAST(cro2 AS bigint) AS cro2,
    CAST(cro3 AS bigint) AS cro3,
    CAST(cro4 AS bigint) AS cro4,
    CAST(cro5 AS bigint) AS cro5,
    CAST(cro7 AS bigint) AS cro7,
    CAST(cro8 AS bigint) AS cro8,
    CAST(cro9 AS bigint) AS cro9,
    date,
    CAST(cro10 AS bigint) AS cro10,
    CAST(cro11 AS bigint) AS cro11,
    CAST(cro12 AS bigint) AS cro12,
    cro7a,
    cro8a,
    b_secb,
    caseid,
    cro13a,
    CAST(cro13b AS bigint) AS cro13b,
    CAST(cro13c AS bigint) AS cro13c,
    n_seca,
    n_secc,
    endtime,
    meeting,
    n1_secd,
    n1_sece,
    n2_sece,
    n3_sece,
    n4_sece,
    n5_sece,
    remarks,
    woredas,
    deviceid,
    CAST(duration AS bigint) AS duration,
    CAST(expected AS bigint) AS expected,
    username,
    starttime,
    CAST(cro13av_na AS bigint) AS cro13av_na,
    "instanceID",
    device_info,
    CAST(malepresent AS bigint) AS malepresent,
    school_name,
    coach_gender,
    date_coaching,
    CAST(femalepresent AS bigint) AS femalepresent,
    observer_role,
    review_status,
    role_coaching,
    "CompletionDate",
    "SubmissionDate",
    coachee_gender,
    devicephonenum,
    forms_ethiopia,
    review_quality,
    teacher_gender,
    teacher_others,
    zones_ethiopia,
    CAST(cro13av_dev_cro AS bigint) AS cro13av_dev_cro,
    CAST(formdef_version AS bigint) AS formdef_version,
    observer_gender,
    observer_others,
    region_ethiopia as region,
    facilitator_role,
    meeting_coaching,
    observation_term,
    remarks_coaching,
    CAST(duration_coaching AS bigint) AS duration_coaching,
    name_of_the_coach,
    remarks_classroom,
    facilitator_gender,
    facilitator_others,
    name_of_the_coachee,
    facilitator_role_coaching
FROM cte
