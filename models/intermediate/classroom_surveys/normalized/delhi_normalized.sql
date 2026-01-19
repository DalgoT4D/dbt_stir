{{ config(
  materialized='table',
) }}

SELECT
    -- Core survey response columns
    CAST(c1 AS bigint) AS c1,
    CAST(c2 AS bigint) AS c2,
    CAST(c3 AS bigint) AS c3,
    c1a,
    c2a,
    
    -- Engagement columns
    CAST(e1 AS bigint) AS e1,
    CAST(e2 AS bigint) AS e2,
    
    -- Safety columns
    CAST(s1 AS bigint) AS s1,
    CAST(s2 AS bigint) AS s2,
    CAST(s3 AS bigint) AS s3,
    CAST(s4 AS bigint) AS s4,
    
    -- Self-esteem columns
    CAST(se1 AS bigint) AS se1,
    CAST(se2 AS bigint) AS se2,
    CAST(se3 AS bigint) AS se3,
    CAST(se4 AS bigint) AS se4,
    CAST(se5 AS bigint) AS se5,
    
    -- Activity columns
    CAST("AD1" AS bigint) AS "AD1",
    CAST("AD2" AS bigint) AS "AD2", 
    CAST("AD3" AS bigint) AS "AD3",
    CAST("AD4" AS bigint) AS "AD4",
    CAST("AD5" AS bigint) AS "AD5",
    "AD6",
    CAST("AD7" AS bigint) AS "AD7",
    CAST("AD8" AS bigint) AS "AD8",
    CAST("AD9" AS bigint) AS "AD9",
    
    -- Self-reflection columns
    CAST(sr1 AS bigint) AS sr1,
    CAST(sr2 AS bigint) AS sr2,
    CAST(sr3 AS bigint) AS sr3,
    CAST(sr4 AS bigint) AS sr4,
    CAST(sr5 AS bigint) AS sr5,
    CAST(sr6 AS bigint) AS sr6,
    
    -- Classroom observation columns
    CAST(cro1 AS bigint) AS cro1,
    CAST(cro2 AS bigint) AS cro2,
    CAST(cro3 AS bigint) AS cro3,
    CAST(cro4 AS bigint) AS cro4,
    CAST(cro5 AS bigint) AS cro5,
    CAST(cro7 AS bigint) AS cro7,
    CAST(cro8 AS bigint) AS cro8,
    CAST(cro9 AS bigint) AS cro9,
    CAST(cro10 AS bigint) AS cro10,
    CAST(cro11 AS bigint) AS cro11,
    CAST(cro12 AS bigint) AS cro12,
    cro7a,
    cro8a,
    cro13a,
    CAST(cro13b AS bigint) AS cro13b,
    CAST(cro13c AS bigint) AS cro13c,
    cro13ai,
    
    -- Coaching call columns
    CAST(cc1 AS bigint) AS cc1,
    CAST(cc2 AS bigint) AS cc2,
    CAST(cc3 AS bigint) AS cc3,
    CAST(cc4 AS bigint) AS cc4,
    CAST(cc5 AS bigint) AS cc5,
    
    -- Metadata columns
    "KEY",
    caseid,
    forms,
    date,
    starttime,
    endtime,
    "SubmissionDate",
    deviceid,
    CAST(duration AS bigint) AS duration,
    CAST(expected AS bigint) AS expected,
    username,
    diet_delhi,
    formdef_id,
    "instanceID",
    device_info,
    CAST(malepresent AS bigint) AS malepresent,
    CAST(femalepresent AS bigint) AS femalepresent,
    CAST(formdef_version AS bigint) AS formdef_version,
    
    -- Gender and role columns
    coach_gender,
    coachee_gender,
    teacher_gender,
    observer_role,
    observer_gender,
    facilitator_role,
    facilitator_gender,
    
    -- Text fields
    coaching_type,
    teacher_others,
    meeting,
    facilitator_others,
    observer_others,
    name_of_the_coach,
    name_of_the_coachee,
    review_quality,
    observation_term,
    remarks_coaching,
    remarks_classroom,
    remarks_additional,
    role_coaching,
    meeting_coaching,
    date_coaching,
    CAST(duration_coaching AS bigint) AS duration_coaching,
    remarks,
    program,
    
    -- Specification fields
    coachee_gender_specify,
    coach_gender_specify,
    facilitator_role_coaching,
    
    -- Phone number field
    devicephonenum,
    
    -- Airbyte metadata columns
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta
    
FROM {{ source('source_classroom_surveys', 'delhi') }}