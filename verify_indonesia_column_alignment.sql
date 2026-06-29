-- Verify Indonesia_SCTO Column Alignment
-- This script checks for column mismatches between expected and actual source

-- 1. Get actual columns from source table
WITH actual_columns AS (
    SELECT 
        column_name,
        data_type,
        is_nullable,
        ordinal_position
    FROM information_schema.columns
    WHERE table_schema = 'new_staging'
      AND table_name = 'Indonesia_SCTO'
),

-- 2. Expected columns from indonesia_normalized.sql (SurveyCTO-specific columns)
expected_scto_columns AS (
    SELECT column_name FROM (VALUES
        ('c1'), ('c2'), ('c3'), ('e1'), ('e2'),
        ('s1'), ('s2'), ('s3'), ('s4'),
        ('_id'),
        ('c1a'), ('c2a'),
        ('cc1'), ('cc2'), ('cc3'), ('cc4'), ('cc5'),
        ('gc1'), ('gc2'), ('gc3'), ('gc4'), ('gc5'),
        ('se1'), ('se2'), ('se3'), ('se4'), ('se5'),
        ('cro1'), ('cro2'), ('cro3'), ('cro4'), ('cro5'), ('cro7'), ('cro8'), ('cro9'),
        ('date'), ('ge_1'), ('ge_2'), ('ge_3'), ('ge_4'), ('ge_5'),
        ('cro10'), ('cro11'), ('cro12'), ('cro7a'), ('cro8a'),
        ('cro13a'), ('cro13b'), ('cro13c'), ('cro13av'),
        ('endtime'), ('meeting'),
        ('remarks'),
        ('deviceid'), ('duration'), ('expected'), ('username'), ('programme'),
        ('starttime'),
        ('device_info'),
        ('malepresent'),
        ('type_school'), ('coach_gender'),
        ('date_coaching'), ('femalepresent'),
        ('observer_role'), ('role_coaching'),
        ('coachee_gender'), ('devicephonenum'),
        ('teacher_gender'), ('teacher_others'),
        ('forms_indonesia'),
        ('observer_gender'), ('observer_others'),
        ('facilitator_role'), ('meeting_coaching'), ('observation_term'),
        ('remarks_coaching'), ('duration_coaching'), ('name_of_the_coach'),
        ('remarks_classroom'),
        ('district_indonesia'), ('facilitator_gender'), ('facilitator_others'),
        ('location_indonesia'), ('name_of_the_coachee'),
        ('coach_gender_specify'), ('coachee_gender_specify'),
        ('remarks_group_coaching'),
        ('facilitator_role_coaching'),
        ('_airbyte_raw_id'), ('_airbyte_extracted_at'), ('_airbyte_meta'),
        -- Check for SubmissionDate (contradiction in code)
        ('SubmissionDate')
    ) AS t(column_name)
)

-- 3. Find missing columns (expected but not in source)
SELECT 
    'MISSING IN SOURCE' as issue_type,
    e.column_name as column_name,
    NULL as data_type,
    'Column expected in model but not found in source table' as issue_description
FROM expected_scto_columns e
LEFT JOIN actual_columns a ON LOWER(e.column_name) = LOWER(a.column_name)
WHERE a.column_name IS NULL

UNION ALL

-- 4. Find extra columns (in source but not expected)
SELECT 
    'EXTRA IN SOURCE' as issue_type,
    a.column_name as column_name,
    a.data_type,
    'Column exists in source but not explicitly referenced in model' as issue_description
FROM actual_columns a
LEFT JOIN expected_scto_columns e ON LOWER(a.column_name) = LOWER(e.column_name)
WHERE e.column_name IS NULL
  AND LOWER(a.column_name) NOT IN (
      -- Exclude known Kobo-only columns that are handled as NULL
      'n1', 'n2', 'n3', 'n4', 'end', '_tags', '_uuid', 'start', 
      '_index', '_notes', 'caseid', 'n_seca', 'n_secc', '_status',
      'n1_secd', 'n1_sece', 'n2_sece', 'n2_secf', 'n3_sece', 'n4_sece', 'n5_sece',
      'date_cro', 'formdef_id', 'device_det', 'instanceid', '__version__',
      'phonenumber', '_submitted_by', 'interview_dur', 'review_quality',
      'formdef_version', '_submission_time', '_validation_status',
      'cro13a_digital_learning', 'cro13av_digital_learning',
      'cro13a_settlers___stirrers', 'cro13a_pairwork___groupwork',
      'cro13av_settlers___stirrers', 'cro13av_pairwork___groupwork',
      'cro13a_collab___coop_learning', 'cro13av_collab___coop_learning',
      'cro13a_differentiated_instruction', 'cro13av_differentiated_instruction'
  )

ORDER BY issue_type, column_name;

-- 5. Summary statistics
SELECT 
    'SUMMARY' as report_section,
    (SELECT COUNT(*) FROM actual_columns) as actual_column_count,
    (SELECT COUNT(*) FROM expected_scto_columns) as expected_column_count,
    (SELECT COUNT(*) FROM expected_scto_columns e
     LEFT JOIN actual_columns a ON LOWER(e.column_name) = LOWER(a.column_name)
     WHERE a.column_name IS NULL) as missing_columns,
    (SELECT COUNT(*) FROM actual_columns a
     LEFT JOIN expected_scto_columns e ON LOWER(a.column_name) = LOWER(e.column_name)
     WHERE e.column_name IS NULL
       AND LOWER(a.column_name) NOT IN ('n1', 'n2', 'n3', 'n4', 'end', '_tags', '_uuid', 'start', 
                                        '_index', '_notes', 'caseid', 'n_seca', 'n_secc', '_status',
                                        'n1_secd', 'n1_sece', 'n2_sece', 'n2_secf', 'n3_sece', 'n4_sece', 'n5_sece',
                                        'date_cro', 'formdef_id', 'device_det', 'instanceid', '__version__',
                                        'phonenumber', '_submitted_by', 'interview_dur', 'review_quality',
                                        'formdef_version', '_submission_time', '_validation_status',
                                        'cro13a_digital_learning', 'cro13av_digital_learning',
                                        'cro13a_settlers___stirrers', 'cro13a_pairwork___groupwork',
                                        'cro13av_settlers___stirrers', 'cro13av_pairwork___groupwork',
                                        'cro13a_collab___coop_learning', 'cro13av_collab___coop_learning',
                                        'cro13a_differentiated_instruction', 'cro13av_differentiated_instruction')
    ) as extra_columns;

-- 6. Check SubmissionDate specifically (resolve contradiction)
SELECT 
    'SUBMISSIONDATE CHECK' as check_type,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM actual_columns 
            WHERE LOWER(column_name) = 'submissiondate'
        ) THEN 'EXISTS in source'
        ELSE 'DOES NOT EXIST in source'
    END as submissiondate_status,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM actual_columns 
            WHERE column_name = 'SubmissionDate'
        ) THEN 'EXISTS (case-sensitive)'
        ELSE 'Does not exist (case-sensitive)'
    END as submissiondate_case_status;

-- 7. Check critical columns
SELECT 
    'CRITICAL COLUMNS CHECK' as check_type,
    column_name,
    CASE WHEN column_name IS NOT NULL THEN 'EXISTS' ELSE 'MISSING' END as status
FROM (
    SELECT 
        MAX(CASE WHEN LOWER(column_name) = '_id' THEN column_name END) as column_name
    FROM actual_columns
    UNION ALL
    SELECT MAX(CASE WHEN LOWER(column_name) = 'forms_indonesia' THEN column_name END)
    FROM actual_columns
    UNION ALL
    SELECT MAX(CASE WHEN LOWER(column_name) = 'location_indonesia' THEN column_name END)
    FROM actual_columns
    UNION ALL
    SELECT MAX(CASE WHEN LOWER(column_name) = 'district_indonesia' THEN column_name END)
    FROM actual_columns
    UNION ALL
    SELECT MAX(CASE WHEN LOWER(column_name) = 'starttime' THEN column_name END)
    FROM actual_columns
    UNION ALL
    SELECT MAX(CASE WHEN LOWER(column_name) = 'date' THEN column_name END)
    FROM actual_columns
    UNION ALL
    SELECT MAX(CASE WHEN LOWER(column_name) = 'deviceid' THEN column_name END)
    FROM actual_columns
) critical_check;
