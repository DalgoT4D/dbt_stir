-- Check if the questioned fields exist in Tamil Nadu and Karnataka source tables

-- Tamil Nadu columns check
SELECT 
    'TN_Data' AS table_name,
    column_name,
    data_type
FROM information_schema.columns 
WHERE table_schema = 'new_staging' 
  AND table_name = 'TN_Data'
  AND column_name IN (
      'n1', 'n2', 'n3', 'n4', 
      'n_seca', 'n_secc',
      'n1_secd', 'n1_sece', 'n2_sece', 'n2_secf', 'n3_sece', 'n4_sece', 'n5_sece',
      '_id', '_uuid', '_tags', '_index', '_notes', '_status', 
      '__version__', '_submitted_by', '_submission_time', '_validation_status',
      'phonenumber', 'device_det', 'interview_dur', 'date_cro',
      'start', 'end'
  )
ORDER BY column_name;

-- Karnataka columns check
SELECT 
    'KT_Data' AS table_name,
    column_name,
    data_type
FROM information_schema.columns 
WHERE table_schema = 'new_staging' 
  AND table_name = 'KT_Data'
  AND column_name IN (
      'n1', 'n2', 'n3', 'n4', 
      'n_seca', 'n_secc',
      'n1_secd', 'n1_sece', 'n2_sece', 'n2_secf', 'n3_sece', 'n4_sece', 'n5_sece',
      '_id', '_uuid', '_tags', '_index', '_notes', '_status', 
      '__version__', '_submitted_by', '_submission_time', '_validation_status',
      'phonenumber', 'device_det', 'interview_dur', 'date_cro',
      'start', 'end'
  )
ORDER BY column_name;

-- Check for any cro13a or cro13av variant columns with slashes
SELECT 
    'TN_Data' AS table_name,
    column_name
FROM information_schema.columns 
WHERE table_schema = 'new_staging' 
  AND table_name = 'TN_Data'
  AND (column_name LIKE 'cro13a/%' OR column_name LIKE 'cro13av/%')
ORDER BY column_name;

SELECT 
    'KT_Data' AS table_name,
    column_name
FROM information_schema.columns 
WHERE table_schema = 'new_staging' 
  AND table_name = 'KT_Data'
  AND (column_name LIKE 'cro13a/%' OR column_name LIKE 'cro13av/%')
ORDER BY column_name;
