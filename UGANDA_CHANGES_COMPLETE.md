# Uganda Model Updates - Complete Summary

## ✅ All Changes Applied

### 1. Fixed KEY Generation
- ✅ Added `surveycto_data_raw` CTE with row numbering using `"KEY"`
- ✅ Changed KEY generation from `_id` to `"KEY"` column
- ✅ Added `CAST(NULL AS varchar) AS _id` in SurveyCTO CTE

### 2. Added Missing SurveyCTO Columns
- ✅ `caseid`
- ✅ `instanceID`
- ✅ `formdef_id`
- ✅ `formdef_version`
- ✅ `review_quality`
- ✅ `"@"` (special character column)
- ✅ `"SubmissionDate"`

### 3. Fixed Kobo cro13aiii Variants
- ✅ Properly quoted all cro13aiii variant columns with slashes
- ✅ Added NULL casts in SurveyCTO section (SurveyCTO only has base cro13aiii column)

### 4. Added Required Columns for Production
- ✅ `cro13ai` → NULL (both sections)
- ✅ `cro13aiv` → NULL (both sections)

### 5. Updated Cleaned Model
- ✅ Added SubmissionDate handling in `int_classroom_surveys_uganda.sql`
- ✅ Uses SubmissionDate first, then falls back to _submission_time

### 6. Column Alignment
- ✅ Both CTEs produce columns in same order
- ✅ Both end with: cro13aiii variants → coachee_gender_specify → _airbyte columns
- ✅ UNION ALL compatibility maintained

## Column Structure

### SurveyCTO Uganda
- Has: `SubmissionDate`, `KEY`, `caseid`, `instanceID`, `formdef_id`, `formdef_version`, `review_quality`, `"@"`, `cro13aiii` (base)
- Missing variants: All cro13aiii variants (cast as NULL)

### Kobo Uganda
- Has: `_id`, cro13aiii variants with slashes, `_Location_*` columns, `_submission_time`
- Missing: `SubmissionDate`, `KEY`, `caseid`, `instanceID`, `formdef_id`, `formdef_version`, `review_quality`, `"@"`

## Key Differences from Indonesia

1. ✅ Uganda has `plname` and `education_level` (Indonesia doesn't)
2. ✅ Uganda has `program` (Indonesia has `programme` which is excluded)
3. ✅ Uganda has cro13aiii variants (properly handled)
4. ✅ Uganda uses both SubmissionDate and _submission_time (cleaned model updated)

## Testing

```bash
dbt compile --select uganda_normalized
dbt run --select uganda_normalized
dbt test --select uganda_normalized int_classroom_surveys_uganda
```

## Files Modified

1. ✅ `models/intermediate/classroom_surveys/normalized/uganda_normalized.sql`
   - Fixed KEY generation
   - Added missing SurveyCTO columns
   - Fixed Kobo cro13aiii variants
   - Added cro13ai and cro13aiv

2. ✅ `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_uganda.sql`
   - Added SubmissionDate handling with _submission_time fallback

## Status

✅ **All Uganda updates complete** - Ready for testing
