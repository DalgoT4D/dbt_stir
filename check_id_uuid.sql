-- Quick check of _id and _uuid columns from Indonesia_SCTO and Uganda_SCTO

-- Indonesia_SCTO: Sample data
SELECT 
    'Indonesia_SCTO' AS source_table,
    _id,
    _uuid,
    deviceid,
    starttime
FROM new_staging."Indonesia_SCTO"
WHERE _id IS NOT NULL OR _uuid IS NOT NULL
LIMIT 10;

-- Uganda_SCTO: Sample data  
SELECT 
    'Uganda_SCTO' AS source_table,
    _id,
    _uuid,
    deviceid,
    starttime
FROM new_staging."Uganda_SCTO"
WHERE _id IS NOT NULL OR _uuid IS NOT NULL
LIMIT 10;
