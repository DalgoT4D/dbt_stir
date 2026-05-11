-- Simple extraction of _id and _uuid sample data
-- Run each query separately or combine as needed

-- Indonesia_SCTO: Sample rows with _id and _uuid
SELECT 
    _id,
    _uuid,
    deviceid,
    starttime,
    date,
    forms_indonesia,
    location_indonesia
FROM new_staging."Indonesia_SCTO"
WHERE _id IS NOT NULL OR _uuid IS NOT NULL
ORDER BY starttime DESC NULLS LAST
LIMIT 20;

-- Uganda_SCTO: Sample rows with _id and _uuid
SELECT 
    _id,
    _uuid,
    deviceid,
    starttime,
    date,
    forms_uganda,
    location_uganda
FROM new_staging."Uganda_SCTO"
WHERE _id IS NOT NULL OR _uuid IS NOT NULL
ORDER BY starttime DESC NULLS LAST
LIMIT 20;

-- Summary statistics
SELECT 
    'Indonesia_SCTO' AS source,
    COUNT(*) AS total_rows,
    COUNT(_id) AS _id_not_null_count,
    COUNT(_uuid) AS _uuid_not_null_count,
    COUNT(DISTINCT _id) AS distinct_id_count,
    COUNT(DISTINCT _uuid) AS distinct_uuid_count,
    COUNT(*) FILTER (WHERE _id IS NULL) AS id_null_count,
    COUNT(*) FILTER (WHERE _uuid IS NULL) AS uuid_null_count
FROM new_staging."Indonesia_SCTO"
UNION ALL
SELECT 
    'Uganda_SCTO' AS source,
    COUNT(*) AS total_rows,
    COUNT(_id) AS _id_not_null_count,
    COUNT(_uuid) AS _uuid_not_null_count,
    COUNT(DISTINCT _id) AS distinct_id_count,
    COUNT(DISTINCT _uuid) AS distinct_uuid_count,
    COUNT(*) FILTER (WHERE _id IS NULL) AS id_null_count,
    COUNT(*) FILTER (WHERE _uuid IS NULL) AS uuid_null_count
FROM new_staging."Uganda_SCTO";
