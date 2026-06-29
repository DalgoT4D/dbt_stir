# Indonesia Model Updates Based on Actual Column Structures

## Changes Applied

### 1. ✅ Fixed KEY Generation
**Issue**: SurveyCTO has `KEY` column, NOT `_id` column

**Changes**:
- Updated `surveycto_data_raw` to use `"KEY"` instead of `_id` for row numbering
- Updated KEY generation to use `"KEY"` column from source: `CONCAT('scto_', "KEY")`
- Added `CAST(NULL AS varchar) AS _id` in SurveyCTO CTE (since _id doesn't exist in SurveyCTO)

**File**: `indonesia_normalized.sql` lines 18, 31-35

### 2. ✅ Added Missing SurveyCTO Columns
**Issue**: Several columns exist in SurveyCTO but were incorrectly cast as NULL

**Columns Fixed**:
- `caseid` - Now included from SurveyCTO (was NULL)
- `instanceID` - Now included from SurveyCTO (was NULL)
- `formdef_id` - Now included from SurveyCTO (was NULL)
- `formdef_version` - Now included from SurveyCTO (was NULL)
- `review_quality` - Now included from SurveyCTO (was NULL)

**File**: `indonesia_normalized.sql` lines 53, 74, 77, 92, 94

### 3. ✅ Fixed Kobo cro13a Variant Columns
**Issue**: Kobo columns have slashes in names (e.g., `cro13a/collab_&_coop_learning`)

**Changes**: Properly quoted column names with aliases:
- `"cro13a/digital_learning" AS cro13a_digital_learning`
- `"cro13av/digital_learning" AS cro13av_digital_learning`
- `"cro13a/settlers_&_stirrers" AS cro13a_settlers___stirrers`
- `"cro13a/pairwork_&_groupwork" AS cro13a_pairwork___groupwork`
- `"cro13av/settlers_&_stirrers" AS cro13av_settlers___stirrers`
- `"cro13av/pairwork_&_groupwork" AS cro13av_pairwork___groupwork`
- `"cro13a/collab_&_coop_learning" AS cro13a_collab___coop_learning`
- `"cro13av/collab_&_coop_learning" AS cro13av_collab___coop_learning`
- `"cro13a/differentiated_instruction" AS cro13a_differentiated_instruction`
- `"cro13av/differentiated_instruction" AS cro13av_differentiated_instruction`

**File**: `indonesia_normalized.sql` lines 185-190

### 4. ✅ Updated Column Count Comments
**Change**: Updated comment from "99 columns" to "~59 columns" for SurveyCTO and "~91 columns" for Kobo

**File**: `indonesia_normalized.sql` line 7

## Column Structure Summary

### SurveyCTO Indonesia Columns (Actual)
- Has: `SubmissionDate`, `KEY`, `caseid`, `instanceID`, `formdef_id`, `formdef_version`, `review_quality`
- Does NOT have: `_id`, `n1-n4`, `_tags`, `_uuid`, `start`, `end`, `_index`, `_notes`, `n_seca`, `n_secc`, `_status`, `n1_secd`, `n1_sece`, `n2_sece`, `n2_secf`, `n3_sece`, `n4_sece`, `n5_sece`, `date_cro`, `device_det`, `phonenumber`, `_submitted_by`, `interview_dur`, `_submission_time`, `_validation_status`, cro13a variants with slashes

### Kobo Indonesia Columns (Actual)
- Has: `_id`, `n1-n4`, `_tags`, `_uuid`, `start`, `end`, `_index`, `_notes`, `n_seca`, `n_secc`, `_status`, `n1_secd`, `n1_sece`, `n2_sece`, `n2_secf`, `n3_sece`, `n4_sece`, `n5_sece`, `date_cro`, `device_det`, `phonenumber`, `_submitted_by`, `interview_dur`, `_submission_time`, `_validation_status`, cro13a variants with slashes
- Does NOT have: `SubmissionDate`, `KEY`, `caseid`, `instanceID`, `formdef_id`, `formdef_version`, `review_quality`

## Testing Checklist

1. ✅ **Compile Model**
   ```bash
   dbt compile --select indonesia_normalized
   ```

2. ✅ **Run Model**
   ```bash
   dbt run --select indonesia_normalized
   ```

3. ✅ **Verify KEY Generation**
   ```sql
   SELECT 
       COUNT(*) as total_rows,
       COUNT("KEY") as has_key,
       COUNT(DISTINCT "KEY") as distinct_keys
   FROM intermediate.indonesia_normalized;
   ```

4. ✅ **Verify Column Alignment**
   ```sql
   -- Check that UNION ALL works (no column mismatch errors)
   SELECT COUNT(*) FROM intermediate.indonesia_normalized;
   ```

5. ✅ **Test Downstream Models**
   ```bash
   dbt test --select indonesia_normalized int_classroom_surveys_indonesia
   ```

## Potential Issues

1. **Airbyte Columns**: Model includes `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta` - if these don't exist in source, may need to remove or handle as NULL

2. **Column Order**: Ensure both CTEs produce columns in exact same order for UNION ALL compatibility

3. **KEY Uniqueness**: Verify KEY generation produces unique values (no duplicates)

## Status

✅ **Updates Complete** - Ready for Testing
