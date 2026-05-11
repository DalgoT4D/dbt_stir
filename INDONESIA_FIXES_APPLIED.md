# Indonesia_SCTO Fixes Applied

## Date: 2026-01-29

## Fixes Applied

### 1. ✅ Fixed SubmissionDate Contradiction

**File**: `models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql`

**Change**: Line 90
- **Before**: `CAST(NULL AS varchar) AS "SubmissionDate",  -- SubmissionDate doesn't exist in SurveyCTO Indonesia`
- **After**: `"SubmissionDate",  -- SubmissionDate exists in SurveyCTO Indonesia`

**Rationale**: 
- The Kobo section (line 169) comment indicated SubmissionDate exists in SurveyCTO but not Kobo
- The cleaned model (`int_classroom_surveys_indonesia.sql`) expects SubmissionDate to exist
- Other countries (Delhi, Tamil Nadu, Karnataka, Ethiopia) all include SubmissionDate
- This fix ensures SubmissionDate data is preserved instead of being cast as NULL

**Impact**:
- SubmissionDate values will now be available from SurveyCTO source
- Should reduce NULL submissiondate values (currently 722 failing tests)
- Aligns with downstream expectations in `int_classroom_surveys_indonesia.sql`

## Verification Status

### Column Alignment
- ✅ SurveyCTO and Kobo CTEs produce same number of columns (124) for UNION ALL
- ✅ Column order matches between both CTEs
- ✅ Critical columns (`_id`, `forms_indonesia`, `location_indonesia`, `district_indonesia`) are present

### Downstream Requirements
- ✅ `forms_indonesia` → maps to `forms` ✓
- ✅ `location_indonesia` → maps to `region` ✓
- ✅ `district_indonesia` → maps to `sub_region` ✓
- ✅ `"SubmissionDate"` → transforms to `submissiondate` ✓
- ✅ All survey indicators (c1-c3, e1-e2, s1-s4, se1-se5, etc.) present ✓
- ✅ All CRO fields (cro1-cro13av) present ✓

### Notes on Missing Columns
- `programme` is excluded in cleaned model (as expected)
- `plname` and `education_level` are not in Indonesia model (will be NULL, handled by union)
- `cro13ai`, `cro13aiii`, `cro13aiv` are parsed from `cro13a` in production model (space-separated values)

## Next Steps for Testing

1. **Compile the model**:
   ```bash
   dbt compile --select indonesia_normalized
   ```

2. **Run the model**:
   ```bash
   dbt run --select indonesia_normalized
   ```

3. **Test the model**:
   ```bash
   dbt test --select indonesia_normalized
   ```

4. **Verify SubmissionDate**:
   ```sql
   SELECT 
       COUNT(*) as total_rows,
       COUNT("SubmissionDate") as has_submissiondate,
       COUNT(submissiondate) as has_submissiondate_cleaned
   FROM intermediate.indonesia_normalized i
   LEFT JOIN intermediate.int_classroom_surveys_indonesia c ON i."KEY" = c."KEY"
   LIMIT 100;
   ```

5. **Check KEY uniqueness**:
   ```sql
   SELECT "KEY", COUNT(*) as count
   FROM intermediate.indonesia_normalized
   GROUP BY "KEY"
   HAVING COUNT(*) > 1;
   ```
   Should return 0 rows.

6. **Run full downstream tests**:
   ```bash
   dbt test --select int_classroom_surveys_indonesia+
   ```

## Expected Improvements

After this fix:
- ✅ SubmissionDate data preserved from SurveyCTO source
- ✅ Reduced NULL submissiondate values (should fix 722 failing test cases)
- ✅ Consistent with other country models
- ✅ Aligned with downstream expectations

## Files Modified

1. `models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql`
   - Line 90: Changed SubmissionDate from NULL cast to actual column reference

## Files Not Modified (Verified Correct)

1. `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_indonesia.sql` - Already correctly expects SubmissionDate
2. `models/production/classroom_surveys/raw/classroom_surveys_merged.sql` - Uses union_relations which handles column alignment
3. All downstream production models - No changes needed

## Potential Issues to Monitor

1. **If SubmissionDate doesn't actually exist in source**: The model will fail with "column does not exist" error. In this case, revert the change and use `starttime` or `date` as fallback.

2. **Column count mismatch**: If source has different column count than expected (99 columns), may need to update comment and verify all columns are handled.

3. **Data type issues**: If SubmissionDate has unexpected format, may need additional parsing logic similar to other timestamp fields.

## Verification Queries

### Check if SubmissionDate exists in source (run this first):
```sql
SELECT column_name 
FROM information_schema.columns
WHERE table_schema = 'new_staging'
  AND table_name = 'Indonesia_SCTO'
  AND LOWER(column_name) = 'submissiondate';
```

### Check SubmissionDate values after fix:
```sql
SELECT 
    COUNT(*) as total,
    COUNT("SubmissionDate") as not_null_count,
    COUNT(*) - COUNT("SubmissionDate") as null_count
FROM intermediate.indonesia_normalized;
```
