-- Direct SQL queries to get row counts (run these in your database)
-- Replace schema names with your actual schema names

-- Uganda: Total rows in combined dataset (SurveyCTO + Kobo)
SELECT 
    'Uganda' AS country,
    COUNT(*) AS total_rows
FROM intermediate.uganda_normalized;

-- Indonesia: Total rows in combined dataset (SurveyCTO + Kobo)  
SELECT 
    'Indonesia' AS country,
    COUNT(*) AS total_rows
FROM intermediate.indonesia_normalized;

-- Both together
SELECT 
    'Uganda' AS country,
    COUNT(*) AS total_rows
FROM intermediate.uganda_normalized
UNION ALL
SELECT 
    'Indonesia' AS country,
    COUNT(*) AS total_rows
FROM intermediate.indonesia_normalized;

-- Optional: Breakdown by source (if you want to verify the split)
-- This would require adding a source identifier column to track which rows came from SurveyCTO vs Kobo
