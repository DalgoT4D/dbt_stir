# Column Usage Analysis for Indonesia_SCTO Fields

## Summary
Most of these columns are **passed through** to downstream models but **not directly used in calculations**. They are included via `dbt_utils.star()` which selects all columns except those explicitly excluded.

## Column-by-Column Analysis

### ✅ **Used in Calculations/Processing**

1. **`_id`** - ✅ **USED**
   - Used for KEY generation in `indonesia_normalized.sql`
   - Used for deduplication logic
   - Critical for data processing

2. **`_submission_time`** - ✅ **USED**
   - Used in deduplication logic (ORDER BY clause)
   - Used in `int_classroom_surveys_indonesia.sql` for `submissiondate` calculation (as fallback)
   - Found in: `indonesia_normalized.sql`, `int_classroom_surveys_uganda.sql`

3. **`starttime`** - ✅ **USED**
   - Used in deduplication ORDER BY
   - Transformed to timestamp in cleaned models
   - Used for sorting and date calculations

4. **`date`** - ✅ **USED**
   - Used for `observation_date` calculation
   - Used in deduplication ORDER BY

### 🔄 **Passed Through (Not Used in Calculations)**

These columns are included in the output via `dbt_utils.star()` but not explicitly used in any calculations:

5. **`n1`, `n2`, `n3`, `n4`** - 🔄 **PASSED THROUGH**
   - Selected in normalized models
   - Passed through to downstream via `dbt_utils.star()`
   - Not used in any calculations or transformations
   - Likely section markers/metadata

6. **`n_seca`, `n_secc`** - 🔄 **PASSED THROUGH**
   - Selected in normalized models
   - Not used in calculations
   - Likely section markers

7. **`n1_secd`, `n1_sece`, `n2_sece`, `n2_secf`, `n3_sece`, `n4_sece`, `n5_sece`** - 🔄 **PASSED THROUGH**
   - Selected in normalized models
   - Not used in calculations
   - Likely section markers

8. **`_uuid`** - 🔄 **PASSED THROUGH**
   - Selected in normalized models
   - Not used for KEY generation (we use `_id` instead)
   - Available in output but not used

9. **`_tags`** - 🔄 **PASSED THROUGH**
   - Selected in normalized models
   - Not used in calculations

10. **`_index`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

11. **`_notes`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

12. **`_status`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

13. **`__version__`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

14. **`_submitted_by`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

15. **`_validation_status`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

16. **`phonenumber`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

17. **`device_det`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

18. **`interview_dur`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

19. **`date_cro`** - 🔄 **PASSED THROUGH**
    - Selected in normalized models
    - Not used in calculations

20. **`start`** - 🔄 **PASSED THROUGH** (Kobo only)
    - Selected in normalized models
    - Not used in calculations

21. **`end`** - 🔄 **PASSED THROUGH** (Kobo only)
    - Selected in normalized models (as `"end"` - reserved word)
    - Not used in calculations

### ❌ **cro13a/cro13av variants - NOT USED**

22. **All `cro13a/*` and `cro13av/*` columns** - ❌ **NOT USED**
    - Examples: `cro13a/digital_learning`, `cro13av/settlers_&_stirrers`, etc.
    - These columns with slashes **don't exist anymore** in the source
    - The source now has columns with underscores: `cro13a_digital_learning`, `cro13av_settlers___stirrers`
    - Selected in normalized models
    - Passed through to downstream
    - **Not used in any calculations or transformations**
    - These are variants of teaching strategies/methods but are not processed

## How `dbt_utils.star()` Works

In `int_classroom_surveys_indonesia.sql`:

```sql
{{ dbt_utils.star(from= ref('indonesia_normalized'), 
   except=['programme', 'district_kota_kediri', 'location_indonesia', 
           'district_indonesia', 's1', 's2', 's3', 'e1', 'e2','c1', 
           'c1a', 'c2', 'c2a', 'c3', 'se1', 'se2', 'se3', 'date', 
           'date_coaching','starttime','endtime','submissiondate',
           '"CompletionDate"', '_airbyte_indonesia_stir_bm_2022_hashid']) }}
```

This means:
- **ALL columns** from `indonesia_normalized` are selected
- **EXCEPT** the ones listed (which are handled separately with transformations)
- So all the `n*`, `_*`, `cro13a*`, `cro13av*` columns are included in the output
- They're available for downstream use but not actively processed

## Recommendation

### Keep These Columns:
- ✅ `_id` - Critical for KEY generation
- ✅ `_submission_time` - Used in deduplication and date logic
- ✅ `starttime`, `date` - Used in calculations
- 🔄 All `n*` columns - Likely section markers, may be needed for reference
- 🔄 `_uuid`, `_tags`, `_index`, `_notes`, `_status` - Metadata that may be useful
- 🔄 `__version__`, `_submitted_by`, `_validation_status` - Audit trail

### Consider Excluding (Low Value):
- ❓ `phonenumber`, `device_det`, `interview_dur`, `date_cro` - Device metadata, rarely used
- ❓ `start`, `end` - Kobo-specific, may duplicate `starttime`/`endtime`
- ❓ All `cro13a*` and `cro13av*` variants - Teaching strategy details not currently processed

### Impact of Removing:
- **Low risk** - These columns are passed through but not used in calculations
- **Benefit** - Slightly smaller tables, faster queries
- **Caution** - If users query these tables directly, they may expect these columns to be present
