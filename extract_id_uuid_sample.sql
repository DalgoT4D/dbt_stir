-- Extract sample data for _id and _uuid from Indonesia_SCTO and Uganda_SCTO
-- This script queries the source tables directly

-- Sample data from Indonesia_SCTO
SELECT 
    '_id' AS column_name,
    _id AS sample_value,
    COUNT(*) AS total_count,
    COUNT(DISTINCT _id) AS distinct_count,
    COUNT(*) FILTER (WHERE _id IS NULL) AS null_count
FROM new_staging."Indonesia_SCTO"
GROUP BY _id
ORDER BY total_count DESC
LIMIT 10;

SELECT 
    '_uuid' AS column_name,
    _uuid AS sample_value,
    COUNT(*) AS total_count,
    COUNT(DISTINCT _uuid) AS distinct_count,
    COUNT(*) FILTER (WHERE _uuid IS NULL) AS null_count
FROM new_staging."Indonesia_SCTO"
GROUP BY _uuid
ORDER BY total_count DESC
LIMIT 10;

-- Sample data from Uganda_SCTO
SELECT 
    '_id' AS column_name,
    _id AS sample_value,
    COUNT(*) AS total_count,
    COUNT(DISTINCT _id) AS distinct_count,
    COUNT(*) FILTER (WHERE _id IS NULL) AS null_count
FROM new_staging."Uganda_SCTO"
GROUP BY _id
ORDER BY total_count DESC
LIMIT 10;

SELECT 
    '_uuid' AS column_name,
    _uuid AS sample_value,
    COUNT(*) AS total_count,
    COUNT(DISTINCT _uuid) AS distinct_count,
    COUNT(*) FILTER (WHERE _uuid IS NULL) AS null_count
FROM new_staging."Uganda_SCTO"
GROUP BY _uuid
ORDER BY total_count DESC
LIMIT 10;

-- Combined view: Sample rows with both _id and _uuid
SELECT 
    'Indonesia_SCTO' AS source_table,
    _id,
    _uuid,
    deviceid,
    starttime,
    date
FROM new_staging."Indonesia_SCTO"
WHERE _id IS NOT NULL OR _uuid IS NOT NULL
ORDER BY starttime DESC NULLS LAST, date DESC NULLS LAST
LIMIT 20;

SELECT 
    'Uganda_SCTO' AS source_table,
    _id,
    _uuid,
    deviceid,
    starttime,
    date
FROM new_staging."Uganda_SCTO"
WHERE _id IS NOT NULL OR _uuid IS NOT NULL
ORDER BY starttime DESC NULLS LAST, date DESC NULLS LAST
LIMIT 20;
