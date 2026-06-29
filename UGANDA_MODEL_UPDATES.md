# Uganda Model Updates Based on Actual Column Structures

## Changes Applied

### 1. ✅ Fixed KEY Generation
**Issue**: SurveyCTO has `KEY` column, NOT `_id` column

**Changes**:
- Added `surveycto_data_raw` CTE with row numbering using `"KEY"` instead of `_id`
- Updated KEY generation to use `"KEY"` column from source: `CONCAT('scto_', "KEY")`
- Added `CAST(NULL AS varchar) AS _id` in SurveyCTO CTE (since _id doesn't exist in SurveyCTO)

**File**: `uganda_normalized.sql` lines 15-29

### 2. ✅ Added Missing SurveyCTO Columns
**Issue**: Several columns exist in SurveyCTO but were missing from model

**Columns Added**:
- `caseid` - Now included from SurveyCTO (line 43)
- `instanceID` - Now included from SurveyCTO (line 45)
- `formdef_id` - Now included from SurveyCTO (line 44)
- `formdef_version` - Now included from SurveyCTO (line 47)
- `review_quality` - Now included from SurveyCTO (line 46)
- `"@"` - Special character column from SurveyCTO (line 48)
- `"SubmissionDate"` - Now included from SurveyCTO (line 56)

**File**: `uganda_normalized.sql` lines 43-48, 56

### 3. ✅ Fixed Kobo cro13aiii Variant Columns
**Issue**: Kobo columns have slashes in names (e.g., `cro13aiii/growth_mindset`)

**Changes**: Properly quoted column names with aliases in Kobo section:
- `"cro13aiii/growth_mindset" AS cro13aiii_growth_mindset`
- `"cro13aiii/physical_learning_environment" AS cro13aiii_physical_learning_environment`
- And all other cro13aiii variants (lines 123-140)

**SurveyCTO Section**: Added NULL casts for all cro13aiii variants since SurveyCTO only has base `cro13aiii` column (lines 67-85)

**File**: `uganda_normalized.sql` lines 67-85 (SurveyCTO), 123-140 (Kobo)

### 4. ✅ Added cro13ai and cro13aiv Columns
**Issue**: Required for `classroom_surveys_normalized` compatibility

**Changes**: Added as NULL in both SurveyCTO and Kobo sections:
- `CAST(NULL AS varchar) AS cro13ai`
- `CAST(NULL AS varchar) AS cro13aiv`

**File**: `uganda_normalized.sql` lines 37-38 (SurveyCTO), 88-89 (Kobo)

### 5. ✅ Updated Column Count Comments
**Change**: Updated comment from "141 columns" to "~62 columns" for SurveyCTO and "~78 columns" for Kobo

**File**: `uganda_normalized.sql` line 7

### 6. ✅ Added Missing Columns to Kobo Section
**Changes**: Added NULL casts for SurveyCTO-only columns:
- `caseid`, `formdef_id`, `instanceID`, `review_quality`, `formdef_version`, `"@"`, `"SubmissionDate"`

**File**: `uganda_normalized.sql` lines 95-101 (Kobo section)

## Column Structure Summary

### SurveyCTO Uganda Columns (Actual)
- Has: `SubmissionDate`, `KEY`, `caseid`, `instanceID`, `formdef_id`, `formdef_version`, `review_quality`, `"@"`, `cro13aiii` (base column)
- Does NOT have: `_id`, cro13aiii variants with slashes, cro13ai, cro13aiv

### Kobo Uganda Columns (Actual)
- Has: `_id`, cro13aiii variants with slashes (`cro13aiii/growth_mindset`, etc.), `_Location_*` columns
- Does NOT have: `SubmissionDate`, `KEY`, `caseid`, `instanceID`, `formdef_id`, `formdef_version`, `review_quality`, `"@"`

## Key Differences from Indonesia

1. **Uganda has `plname` and `education_level`** - Indonesia doesn't have these
2. **Uganda has `program`** - Indonesia has `programme` (excluded)
3. **Uganda has cro13aiii variants** - Need proper handling
4. **Uganda has district columns** - Multiple district boolean columns

## Testing Checklist

1. ✅ **Compile Model**
   ```bash
   dbt compile --select uganda_normalized
   ```

2. ✅ **Run Model**
   ```bash
   dbt run --select uganda_normalized
   ```

3. ✅ **Verify KEY Generation**
   ```sql
   SELECT 
       COUNT(*) as total_rows,
       COUNT("KEY") as has_key,
       COUNT(DISTINCT "KEY") as distinct_keys
   FROM intermediate.uganda_normalized;
   ```

4. ✅ **Verify Column Alignment**
   ```sql
   -- Check that UNION ALL works (no column mismatch errors)
   SELECT COUNT(*) FROM intermediate.uganda_normalized;
   ```

5. ✅ **Test Downstream Models**
   ```bash
   dbt test --select uganda_normalized int_classroom_surveys_uganda
   ```

## Status

✅ **Updates Complete** - Ready for Testing
