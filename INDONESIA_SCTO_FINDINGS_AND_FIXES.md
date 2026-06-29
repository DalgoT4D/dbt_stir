# Indonesia_SCTO Source Change - Findings and Recommended Fixes

## Executive Summary

Analysis of `indonesia_normalized.sql` reveals a **critical contradiction** regarding `SubmissionDate` that needs immediate resolution. The model structure appears sound, but verification of actual source columns is required.

## Critical Issue: SubmissionDate Contradiction

### Problem
There is a contradiction between two files:

1. **indonesia_normalized.sql (line 90)**:
   ```sql
   CAST(NULL AS varchar) AS "SubmissionDate",  -- SubmissionDate doesn't exist in SurveyCTO Indonesia
   ```

2. **int_classroom_surveys_indonesia.sql (line 56)**:
   ```sql
   -- Handle SubmissionDate (exists in SurveyCTO, NULL in Kobo)
   ```

### Impact
- **If SubmissionDate exists**: Data is being lost (cast as NULL)
- **If SubmissionDate doesn't exist**: Cleaned model will always produce NULL submissiondate (722 NULLs already failing tests)
- **Test failures**: `not_null_int_classroom_surveys_indonesia_submissiondate` is failing with 722 NULL values

### Comparison with Other Countries
- **Delhi**: Includes `"SubmissionDate"` directly in normalized model
- **Tamil Nadu**: Uses `"SubmissionDate"` → `submissiondate` transformation
- **Karnataka**: Uses `"SubmissionDate"` → `submissiondate` transformation  
- **Ethiopia**: Uses `"SubmissionDate"` → `submissiondate` transformation
- **Uganda**: Uses `_submission_time` (different field name)

**Conclusion**: Most countries have SubmissionDate, suggesting Indonesia_SCTO likely has it too.

## Column Alignment Analysis

### Expected Columns from SurveyCTO (56 columns)

The model explicitly references these columns from `Indonesia_SCTO`:

#### Critical for KEY Generation
- `_id` - Primary identifier
- `deviceid` - Fallback for KEY
- `starttime` - Fallback for row numbering
- `date` - Fallback for row numbering

#### Critical for Downstream Mapping
- `forms_indonesia` → maps to `forms`
- `location_indonesia` → maps to `region`
- `district_indonesia` → maps to `sub_region`

#### Survey Indicators (Required for unpivoting)
- `c1`, `c2`, `c3`, `c1a`, `c2a` - Critical thinking
- `e1`, `e2` - Engagement
- `s1`, `s2`, `s3`, `s4` - Safety
- `se1`, `se2`, `se3`, `se4`, `se5` - Self-esteem
- `cc1`, `cc2`, `cc3`, `cc4`, `cc5` - Coaching calls
- `gc1`, `gc2`, `gc3`, `gc4`, `gc5` - Group coaching
- `cro1` through `cro13av` - Classroom observations

#### Metadata Columns
- All other columns listed in `INDONESIA_SCTO_COLUMN_ANALYSIS.md`

### UNION ALL Compatibility

The model creates 124 columns total:
- SurveyCTO columns: Explicitly listed (56 actual columns)
- Kobo-only columns: Cast as NULL (68 columns)
- **Total**: 124 columns for UNION ALL compatibility

Both `surveycto_data` and `kobo_data` CTEs produce the same 124 columns in the same order ✅

## Recommended Actions

### 1. IMMEDIATE: Resolve SubmissionDate Issue

**Step 1**: Check if `SubmissionDate` exists in source
```sql
SELECT column_name 
FROM information_schema.columns
WHERE table_schema = 'new_staging'
  AND table_name = 'Indonesia_SCTO'
  AND LOWER(column_name) = 'submissiondate';
```

**Step 2A**: If SubmissionDate EXISTS
- Update `indonesia_normalized.sql` line 90:
  ```sql
  "SubmissionDate",  -- SubmissionDate exists in SurveyCTO Indonesia
  ```
- Update `indonesia_normalized.sql` line 169 (Kobo section):
  ```sql
  CAST(NULL AS varchar) AS "SubmissionDate",  -- doesn't exist in Kobo
  ```

**Step 2B**: If SubmissionDate DOES NOT EXIST
- Update `int_classroom_surveys_indonesia.sql` line 56 comment:
  ```sql
  -- Handle SubmissionDate (doesn't exist in SurveyCTO or Kobo, using starttime fallback)
  ```
- Consider using `starttime` or `date` as fallback for submissiondate

### 2. Verify All Expected Columns Exist

Run `verify_indonesia_column_alignment.sql` to:
- Identify missing columns
- Identify extra columns
- Check column types
- Verify critical columns

### 3. Check for New Columns

If source has NEW columns:
- Add to `surveycto_data` CTE
- Add corresponding NULL/value in `kobo_data` CTE
- Update comment about column count if changed

### 4. Verify Column Types

Ensure types match expectations:
- `_id`: varchar/text (for KEY concatenation)
- Timestamp columns: Compatible with parsing logic
- Indicator columns: Compatible with bigint casting

### 5. Test KEY Uniqueness

After fixes, verify:
```sql
SELECT "KEY", COUNT(*) as count
FROM intermediate.indonesia_normalized
GROUP BY "KEY"
HAVING COUNT(*) > 1;
```

Should return 0 rows.

## Code Changes Required

### Change 1: Fix SubmissionDate in indonesia_normalized.sql

**IF SubmissionDate EXISTS in source:**

```sql
-- Line 90: Change from:
observer_role, role_coaching, CAST(NULL AS varchar) AS "SubmissionDate",  -- SubmissionDate doesn't exist in SurveyCTO Indonesia

-- To:
observer_role, role_coaching, "SubmissionDate",  -- SubmissionDate exists in SurveyCTO Indonesia
```

**IF SubmissionDate DOES NOT EXIST:**

Keep as is, but update comment in `int_classroom_surveys_indonesia.sql` and consider fallback logic.

### Change 2: Update Comment About Column Count

If actual column count differs from 99:
```sql
-- Line 7: Update if needed
-- SurveyCTO has [ACTUAL_COUNT] columns, Kobo has 131 columns
```

## Testing Checklist

After making changes:

- [ ] `dbt compile --select indonesia_normalized` succeeds
- [ ] `dbt run --select indonesia_normalized` completes
- [ ] `dbt run --select int_classroom_surveys_indonesia` completes
- [ ] `dbt test --select indonesia_normalized` passes
- [ ] `dbt test --select int_classroom_surveys_indonesia` passes
- [ ] KEY uniqueness verified (no duplicates)
- [ ] submissiondate has fewer NULLs (if SubmissionDate fix applied)
- [ ] Downstream models (`classroom_surveys_merged`+) still work

## Files Created for Investigation

1. **INDONESIA_SCTO_COLUMN_ANALYSIS.md** - Complete column mapping analysis
2. **verify_indonesia_column_alignment.sql** - SQL script to check column mismatches
3. **INDONESIA_SCTO_CHANGE_CHECKLIST.md** - Step-by-step investigation guide
4. **INDONESIA_SCTO_CHANGE_ANALYSIS.md** - Original impact analysis

## Next Steps

1. **Run verification script**: Execute `verify_indonesia_column_alignment.sql` against the database
2. **Resolve SubmissionDate**: Based on verification results
3. **Update model**: Apply necessary fixes
4. **Test**: Run full test suite
5. **Document**: Update comments with actual findings

## Notes

- The model structure is well-designed with explicit column lists
- UNION ALL compatibility is maintained between SurveyCTO and Kobo
- Deduplication logic is sound
- The main issue is the SubmissionDate contradiction
- All downstream requirements appear to be met IF columns exist as expected
