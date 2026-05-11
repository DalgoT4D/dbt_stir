# Indonesia_SCTO Source Change - Action Checklist

## Quick Reference

**Source Table**: `new_staging.Indonesia_SCTO`  
**dbt Source**: `source_classroom_surveys.indonesia`  
**Primary Model**: `models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql`

## Step-by-Step Investigation Process

### Step 1: Check Current Schema ✅
**Action**: Run `check_indonesia_scto_schema.sql` to get:
- Current column list and data types
- Row counts
- NULL rates for critical columns
- Data quality metrics

**Command**:
```bash
# Run the schema check script against your database
psql -d your_database -f check_indonesia_scto_schema.sql
```

### Step 2: Compare Expected vs Actual Columns ✅
**Action**: Compare the output from Step 1 with the expected columns listed in:
- `INDONESIA_SCTO_CHANGE_ANALYSIS.md` (section "Critical Columns Expected")
- `models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql` (lines 22-124)

**What to Look For**:
- ❌ Missing columns (will cause SQL errors)
- ⚠️ New columns (may need to be added to model)
- ⚠️ Renamed columns (will cause SQL errors)
- ⚠️ Data type changes (may cause casting issues)

### Step 3: Verify Critical Columns ✅
**Critical columns that MUST exist**:
- `_id` - Used for KEY generation
- `forms_indonesia` - Maps to `forms` downstream
- `location_indonesia` - Maps to `region` downstream  
- `district_indonesia` - Maps to `sub_region` downstream
- `starttime`, `date`, `deviceid` - Used for row numbering fallback

**Check**:
```sql
SELECT 
    COUNT(*) as total_rows,
    COUNT(_id) as has_id,
    COUNT(forms_indonesia) as has_forms,
    COUNT(location_indonesia) as has_location,
    COUNT(district_indonesia) as has_district
FROM new_staging."Indonesia_SCTO";
```

### Step 4: Test dbt Compilation ✅
**Action**: Try to compile the affected models

**Command**:
```bash
# Compile just the Indonesia models
dbt compile --select indonesia_normalized int_classroom_surveys_indonesia

# Or compile all models to see full impact
dbt compile
```

**Expected Outcomes**:
- ✅ **Success**: Schema matches expectations, proceed to Step 5
- ❌ **SQL Error**: Column missing/renamed - see "Fixing Missing Columns" below
- ⚠️ **Warning**: Type mismatches - see "Fixing Type Issues" below

### Step 5: Run Tests ✅
**Action**: Run dbt tests to check data quality

**Command**:
```bash
# Test Indonesia-specific models
dbt test --select indonesia_normalized int_classroom_surveys_indonesia

# Test all downstream models
dbt test --select int_classroom_surveys_indonesia+
```

**Key Tests to Watch**:
- `not_null_int_classroom_surveys_indonesia_submissiondate` (currently failing with 722 NULLs)
- `not_null_int_classroom_surveys_indonesia_region`
- `unique_indonesia_normalized_KEY`
- `not_null_indonesia_normalized_KEY`

### Step 6: Check Row Counts ✅
**Action**: Verify row counts match expectations

**Command**:
```bash
# Run the row count check script
psql -d your_database -f get_row_counts.sql

# Or check manually
dbt run --select indonesia_normalized
# Then query the table to verify row count
```

## Common Issues and Fixes

### Issue 1: Missing Column
**Symptom**: `column "column_name" does not exist`

**Fix**:
1. If column was removed from source:
   - Remove from `indonesia_normalized.sql` column list
   - Replace with `CAST(NULL AS varchar) AS column_name` if needed for UNION compatibility
   
2. If column was renamed:
   - Update column name in `indonesia_normalized.sql`
   - Update any references in `int_classroom_surveys_indonesia.sql`

**Example**:
```sql
-- If 'forms_indonesia' was renamed to 'form_type'
-- Change line 95 in indonesia_normalized.sql from:
forms_indonesia,
-- To:
form_type AS forms_indonesia,
```

### Issue 2: New Column Added
**Symptom**: Column exists in source but not in model

**Fix**:
1. Add column to `surveycto_data` CTE in `indonesia_normalized.sql`
2. Add corresponding NULL or value in `kobo_data` CTE for UNION compatibility
3. Update comment about column count if changed

**Example**:
```sql
-- Add new column after existing columns in surveycto_data CTE
facilitator_role_coaching,
-- Add NULL in kobo_data CTE if it doesn't exist there
CAST(NULL AS varchar) AS facilitator_role_coaching,
```

### Issue 3: Data Type Mismatch
**Symptom**: Type casting errors or test failures

**Fix**:
1. Add explicit CAST in `indonesia_normalized.sql`
2. Ensure compatibility with Kobo data UNION

**Example**:
```sql
-- If _id changed from varchar to integer
CAST(_id AS varchar) AS _id,
```

### Issue 4: NULL Values Increased
**Symptom**: Test failures for `not_null` constraints

**Fix Options**:
1. **Filter NULLs** (if NULLs are invalid):
   ```sql
   WHERE _id IS NOT NULL AND btrim(_id) != ''
   ```

2. **Remove constraint** (if NULLs are valid):
   - Update `models/intermediate/classroom_surveys/schema.yml`
   - Remove `not_null` test for the column

3. **Handle in transformation**:
   ```sql
   COALESCE(forms_indonesia, 'unknown') AS forms_indonesia
   ```

### Issue 5: KEY Generation Issues
**Symptom**: Duplicate KEY values or NULL KEYs

**Fix**:
1. Check `_id` column quality
2. Verify fallback logic in KEY generation
3. Add additional fallback columns if needed

**Example**:
```sql
-- Enhanced KEY generation with more fallbacks
COALESCE(
    CASE WHEN _id IS NOT NULL AND btrim(_id) != '' THEN CONCAT('scto_', _id) ELSE NULL END,
    CASE WHEN deviceid IS NOT NULL AND btrim(deviceid) != '' THEN CONCAT('scto_dev_', deviceid) ELSE NULL END,
    CONCAT('scto_', CAST(row_num AS varchar))
) AS "KEY"
```

## Files to Update (Based on Changes Found)

### If Columns Changed:
- [ ] `models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql`
  - Update column list in `surveycto_data` CTE (lines 22-124)
  - Update comments about column count
  - Update UNION compatibility with Kobo data

### If Column Mappings Changed:
- [ ] `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_indonesia.sql`
  - Update column references (line 8)
  - Update mapping logic (lines 7, 9)

### If Data Quality Changed:
- [ ] `models/intermediate/classroom_surveys/schema.yml`
  - Update test expectations
  - Adjust `not_null` constraints if needed

### If Form Codes Changed:
- [ ] `models/production/classroom_surveys/raw/classroom_surveys_merged.sql`
  - Update CASE statements for form mapping (lines 14, 18-20, 41, 43-44, 47, 49-50, 73, 75-76)

## Validation Checklist

After making changes, verify:

- [ ] `dbt compile` succeeds without errors
- [ ] `dbt run --select indonesia_normalized` completes successfully
- [ ] `dbt run --select int_classroom_surveys_indonesia` completes successfully
- [ ] `dbt test --select indonesia_normalized` passes
- [ ] `dbt test --select int_classroom_surveys_indonesia` passes
- [ ] Row counts match expectations
- [ ] KEY column is unique in `indonesia_normalized`
- [ ] Downstream models (`classroom_surveys_merged` and derived models) still work
- [ ] No new NULL values in critical columns (unless expected)

## Quick Test Commands

```bash
# Full test suite for Indonesia models
dbt test --select indonesia_normalized int_classroom_surveys_indonesia

# Run and test Indonesia pipeline
dbt run --select indonesia_normalized+ && dbt test --select indonesia_normalized+

# Check compilation only (fast)
dbt compile --select indonesia_normalized int_classroom_surveys_indonesia

# Full downstream impact test
dbt test --select int_classroom_surveys_indonesia+
```

## Getting Help

If you encounter issues:
1. Check `INDONESIA_SCTO_CHANGE_ANALYSIS.md` for detailed column list
2. Review `TEST_FAILURES_ANALYSIS.md` for known test issues
3. Compare with similar models (e.g., `uganda_normalized.sql`) for patterns
4. Check dbt logs for specific SQL errors

## Notes

- The model expects **99 columns** from SurveyCTO (per code comment)
- SurveyCTO data is UNION ALL'd with Kobo data (131 columns)
- Deduplication happens based on KEY column
- Changes to timestamp columns may affect deduplication logic
- Form codes are mapped in `classroom_surveys_merged.sql` - verify these still match
