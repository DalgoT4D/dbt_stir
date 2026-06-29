# Indonesia SubmissionDate Fix - Fallback Implementation

## Issue
The `SubmissionDate` column does **NOT** exist in the `Indonesia_SCTO` source table, causing the model to fail with:
```
column "SubmissionDate" does not exist
```

## Solution Applied

### 1. Reverted SubmissionDate in Normalized Model
**File**: `models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql` (Line 90)

- ✅ Reverted to: `CAST(NULL AS varchar) AS "SubmissionDate"`
- ✅ Updated comment to clarify: `-- SubmissionDate doesn't exist in SurveyCTO Indonesia, using starttime/date as fallback`

### 2. Added Fallback Logic in Cleaned Model
**File**: `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_indonesia.sql` (Lines 55-78)

**New Logic**: Uses fallback chain for `submissiondate`:
1. **First**: Try `SubmissionDate` (for Kobo data that might have it)
2. **Fallback 1**: Use `starttime` (parsed as timestamp)
3. **Fallback 2**: Use `date` field (parsed as timestamp)

**Implementation**:
```sql
CASE
  -- Handle SubmissionDate (doesn't exist in SurveyCTO Indonesia, using starttime/date as fallback)
  WHEN "SubmissionDate" IS NOT NULL AND "SubmissionDate" ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
    THEN to_timestamp("SubmissionDate", 'DD/MM/YYYY HH24:MI:SS')
  WHEN "SubmissionDate" IS NOT NULL AND "SubmissionDate" ~ '^\d{2}/\d{2}/\d{4}'
    THEN to_timestamp("SubmissionDate", 'DD/MM/YYYY')
  WHEN "SubmissionDate" IS NOT NULL
    THEN "SubmissionDate"::timestamp
  -- Fallback to starttime if SubmissionDate is NULL (SurveyCTO Indonesia doesn't have SubmissionDate)
  WHEN starttime IS NOT NULL AND btrim(starttime) ~ '^[0-9]+(\.[0-9]+)?$'
    THEN (timestamp '1899-12-30' + (btrim(starttime)::double precision * interval '1 day'))
  WHEN starttime IS NOT NULL AND starttime ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}'
    THEN to_timestamp(btrim(starttime), 'DD/MM/YYYY HH24:MI:SS')
  WHEN starttime IS NOT NULL AND starttime ~ '^\d{2}/\d{2}/\d{4}'
    THEN to_timestamp(btrim(starttime), 'DD/MM/YYYY')
  WHEN starttime IS NOT NULL
    THEN starttime::timestamp
  -- Final fallback to date field
  WHEN date IS NOT NULL AND date ~ '^\d{2}/\d{2}/\d{4}'
    THEN to_timestamp(date, 'DD/MM/YYYY')
  WHEN date IS NOT NULL
    THEN to_date(date, 'YYYY-MM-DD')::timestamp
  ELSE NULL
END AS submissiondate
```

## Expected Results

### Before Fix
- ❌ Model fails: `column "SubmissionDate" does not exist`
- ❌ submissiondate: NULL for all SurveyCTO records (722 NULLs)

### After Fix
- ✅ Model runs successfully
- ✅ submissiondate populated using `starttime` or `date` fallback
- ✅ Should significantly reduce NULL submissiondate values
- ✅ Kobo data (if it has SubmissionDate) will use that value

## Testing

### 1. Run Model
```bash
dbt run --select indonesia_normalized int_classroom_surveys_indonesia
```

### 2. Verify SubmissionDate Fallback
```sql
-- Check submissiondate population
SELECT 
    COUNT(*) as total_rows,
    COUNT(submissiondate) as has_submissiondate,
    COUNT(*) - COUNT(submissiondate) as null_count,
    -- Check which fallback was used
    COUNT(CASE WHEN "SubmissionDate" IS NOT NULL THEN 1 END) as used_submissiondate,
    COUNT(CASE WHEN "SubmissionDate" IS NULL AND starttime IS NOT NULL THEN 1 END) as used_starttime,
    COUNT(CASE WHEN "SubmissionDate" IS NULL AND starttime IS NULL AND date IS NOT NULL THEN 1 END) as used_date
FROM intermediate.int_classroom_surveys_indonesia;
```

### 3. Test Model
```bash
dbt test --select int_classroom_surveys_indonesia
```

**Expected**: 
- ✅ submissiondate should have fewer NULLs
- ✅ submissiondate type test should pass
- ✅ All other tests should pass

## Notes

- **SurveyCTO Indonesia**: Uses `starttime` or `date` as submissiondate source
- **Kobo Indonesia**: May have `SubmissionDate`, will use that if available
- **Fallback Chain**: SubmissionDate → starttime → date → NULL
- **Consistent with Uganda**: Uganda also uses `_submission_time` instead of `SubmissionDate`

## Files Modified

1. ✅ `models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql`
   - Line 90: Reverted SubmissionDate to NULL cast with updated comment

2. ✅ `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_indonesia.sql`
   - Lines 55-78: Added fallback logic for submissiondate

## Status

✅ **Fix Applied** - Ready for Testing
