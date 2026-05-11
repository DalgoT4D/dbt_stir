-- Check Indonesia_SCTO Source Schema and Data Quality
-- Run this to identify changes in the source table structure

-- 1. Get current schema
SELECT 
    column_name,
    data_type,
    is_nullable,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    ordinal_position
FROM information_schema.columns
WHERE table_schema = 'new_staging'
  AND table_name = 'Indonesia_SCTO'
ORDER BY ordinal_position;

-- 2. Get row count
SELECT 
    'Row Count' as metric,
    COUNT(*)::text as value
FROM new_staging."Indonesia_SCTO";

-- 3. Check critical column existence and NULL rates
SELECT 
    'Critical Columns Check' as check_type,
    COUNT(*) as total_rows,
    COUNT(_id) as _id_not_null,
    COUNT(forms_indonesia) as forms_indonesia_not_null,
    COUNT(location_indonesia) as location_indonesia_not_null,
    COUNT(district_indonesia) as district_indonesia_not_null,
    COUNT(starttime) as starttime_not_null,
    COUNT(date) as date_not_null,
    COUNT(deviceid) as deviceid_not_null,
    COUNT(endtime) as endtime_not_null,
    COUNT(date_coaching) as date_coaching_not_null
FROM new_staging."Indonesia_SCTO";

-- 4. Check for duplicate _id values (should be unique for proper KEY generation)
SELECT 
    'Duplicate _id Check' as check_type,
    COUNT(*) as total_rows,
    COUNT(DISTINCT _id) as distinct_ids,
    COUNT(*) - COUNT(DISTINCT _id) as duplicate_count
FROM new_staging."Indonesia_SCTO"
WHERE _id IS NOT NULL AND btrim(_id) != '';

-- 5. Sample data from critical columns
SELECT 
    _id,
    forms_indonesia,
    location_indonesia,
    district_indonesia,
    starttime,
    date,
    deviceid
FROM new_staging."Indonesia_SCTO"
LIMIT 10;

-- 6. Check forms_indonesia values (to verify form codes)
SELECT 
    forms_indonesia,
    COUNT(*) as count
FROM new_staging."Indonesia_SCTO"
WHERE forms_indonesia IS NOT NULL
GROUP BY forms_indonesia
ORDER BY count DESC;

-- 7. Check date format consistency
SELECT 
    'Date Format Check' as check_type,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN date ~ '^\d{2}/\d{2}/\d{4}' THEN 1 END) as dd_mm_yyyy_format,
    COUNT(CASE WHEN date ~ '^\d{4}-\d{2}-\d{2}' THEN 1 END) as yyyy_mm_dd_format,
    COUNT(CASE WHEN date IS NULL THEN 1 END) as null_dates,
    COUNT(CASE WHEN date IS NOT NULL AND date !~ '^\d{2}/\d{2}/\d{4}' AND date !~ '^\d{4}-\d{2}-\d{2}' THEN 1 END) as other_formats
FROM new_staging."Indonesia_SCTO";

-- 8. Expected columns list (from indonesia_normalized.sql)
-- Compare this list with actual columns from query #1
SELECT 
    'Expected Column' as column_name,
    'Expected in SurveyCTO' as note
FROM (VALUES
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
    ('_airbyte_raw_id'), ('_airbyte_extracted_at'), ('_airbyte_meta')
) AS expected_cols(column_name, note);
