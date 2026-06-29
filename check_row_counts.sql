-- Query to check total row counts in combined datasets
-- Run this after dbt run completes

-- Uganda combined dataset (SurveyCTO + Kobo)
SELECT 
    'Uganda' AS country,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT _airbyte_raw_id) AS distinct_records
FROM {{ ref('uganda_normalized') }};

-- Indonesia combined dataset (SurveyCTO + Kobo)
SELECT 
    'Indonesia' AS country,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT _airbyte_raw_id) AS distinct_records
FROM {{ ref('indonesia_normalized') }};

-- Breakdown by source (if you want to verify UNION worked correctly)
-- Note: This requires identifying which rows came from which source
-- You could add a source column to track this if needed
