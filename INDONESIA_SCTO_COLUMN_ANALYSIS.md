# Indonesia_SCTO Column Mapping Analysis

## Critical Issue Found: SubmissionDate Contradiction

### Contradiction Identified
1. **indonesia_normalized.sql line 90**: 
   ```sql
   CAST(NULL AS varchar) AS "SubmissionDate",  -- SubmissionDate doesn't exist in SurveyCTO Indonesia
   ```

2. **int_classroom_surveys_indonesia.sql line 56**: 
   ```sql
   -- Handle SubmissionDate (exists in SurveyCTO, NULL in Kobo)
   ```

**This is contradictory!** The normalized model says SubmissionDate doesn't exist in SurveyCTO, but the cleaned model expects it to exist.

### Impact
- If SubmissionDate doesn't exist in SurveyCTO: The cleaned model will always produce NULL submissiondate values (722 NULLs already failing tests)
- If SubmissionDate exists in SurveyCTO: The normalized model is incorrectly casting it as NULL, losing the data

## Complete Column List from SurveyCTO (Expected)

Based on `indonesia_normalized.sql` lines 22-124, here are ALL columns expected from SurveyCTO:

### Core Indicators (Required for downstream)
1. `c1`, `c2`, `c3` - Critical thinking indicators
2. `c1a`, `c2a` - Additional critical thinking
3. `e1`, `e2` - Engagement indicators  
4. `s1`, `s2`, `s3`, `s4` - Safety indicators
5. `se1`, `se2`, `se3`, `se4`, `se5` - Self-esteem indicators
6. `cc1`, `cc2`, `cc3`, `cc4`, `cc5` - Coaching calls
7. `gc1`, `gc2`, `gc3`, `gc4`, `gc5` - Group coaching
8. `cro1`, `cro2`, `cro3`, `cro4`, `cro5`, `cro7`, `cro8`, `cro9` - Classroom observations
9. `cro10`, `cro11`, `cro12`, `cro7a`, `cro8a` - Additional CRO fields
10. `cro13a`, `cro13b`, `cro13c`, `cro13av` - Extended CRO fields

### Identification & Metadata (Critical)
11. `_id` - **CRITICAL** - Used for KEY generation
12. `deviceid` - Used for KEY fallback and row numbering
13. `starttime` - Used for row numbering and timestamp parsing
14. `date` - Used for row numbering and date parsing
15. `endtime` - End timestamp
16. `date_coaching` - Coaching date

### Geographic & Form Identification (Critical)
17. `forms_indonesia` - **CRITICAL** - Maps to `forms` downstream
18. `location_indonesia` - **CRITICAL** - Maps to `region` downstream
19. `district_indonesia` - **CRITICAL** - Maps to `sub_region` downstream

### Other Metadata
20. `ge_1`, `ge_2`, `ge_3`, `ge_4`, `ge_5` - Gender equity
21. `meeting` - Meeting identifier
22. `remarks` - Qualitative remarks
23. `duration` - Duration
24. `expected` - Expected value
25. `username` - Username
26. `programme` - Programme identifier
27. `device_info` - Device information
28. `malepresent` - Male presence indicator
29. `femalepresent` - Female presence indicator
30. `type_school` - School type
31. `coach_gender` - Coach gender
32. `observer_role` - Observer role
33. `role_coaching` - Coaching role
34. `coachee_gender` - Coachee gender
35. `devicephonenum` - Device phone number
36. `teacher_gender` - Teacher gender
37. `teacher_others` - Other teacher info
38. `observer_gender` - Observer gender
39. `observer_others` - Other observer info
40. `facilitator_role` - Facilitator role
41. `meeting_coaching` - Meeting coaching field
42. `observation_term` - Observation term
43. `remarks_coaching` - Coaching remarks
44. `duration_coaching` - Coaching duration
45. `name_of_the_coach` - Coach name
46. `remarks_classroom` - Classroom remarks
47. `facilitator_gender` - Facilitator gender
48. `facilitator_others` - Other facilitator info
49. `name_of_the_coachee` - Coachee name
50. `coach_gender_specify` - Coach gender specification
51. `coachee_gender_specify` - Coachee gender specification
52. `remarks_group_coaching` - Group coaching remarks
53. `facilitator_role_coaching` - Facilitator role in coaching

### Airbyte Columns (if using Airbyte)
54. `_airbyte_raw_id`
55. `_airbyte_extracted_at`
56. `_airbyte_meta`

### Columns NOT in SurveyCTO (but in Kobo)
These are cast as NULL in surveycto_data:
- `n1`, `n2`, `n3`, `n4`
- `"end"` (reserved word)
- `_tags`, `_uuid`
- `start`
- `_index`, `_notes`
- `caseid`
- `n_seca`, `n_secc`, `_status`
- `n1_secd`, `n1_sece`, `n2_sece`, `n2_secf`, `n3_sece`, `n4_sece`, `n5_sece`
- `date_cro`
- `formdef_id`
- `device_det`
- `"instanceID"`
- `__version__`
- `phonenumber`
- `_submitted_by`
- `interview_dur`
- `"SubmissionDate"` ⚠️ **CONTRADICTION HERE**
- `review_quality`
- `formdef_version`
- `_submission_time`
- `_validation_status`
- `cro13a_digital_learning`, `cro13av_digital_learning`
- `cro13a_settlers___stirrers`, `cro13a_pairwork___groupwork`
- `cro13av_settlers___stirrers`, `cro13av_pairwork___groupwork`
- `cro13a_collab___coop_learning`, `cro13av_collab___coop_learning`
- `cro13a_differentiated_instruction`, `cro13av_differentiated_instruction`

## Column Alignment Check: surveycto_data vs kobo_data

### Total Columns in Final Output
Both CTEs should produce the same number of columns in the same order for UNION ALL to work.

**SurveyCTO CTE columns**: 124 columns (explicitly listed)
**Kobo CTE columns**: 124 columns (explicitly listed)

✅ Column count matches

### Column Order Verification
The columns are in the same order in both CTEs, which is required for UNION ALL.

## Downstream Column Requirements

### From int_classroom_surveys_indonesia.sql
The cleaned model expects these columns from `indonesia_normalized`:
- All columns via `dbt_utils.star()` except: `programme`, `district_kota_kediri`, `location_indonesia`, `district_indonesia`, `s1`, `s2`, `s3`, `e1`, `e2`, `c1`, `c1a`, `c2`, `c2a`, `c3`, `se1`, `se2`, `se3`, `date`, `date_coaching`, `starttime`, `endtime`, `submissiondate`, `"CompletionDate"`, `_airbyte_indonesia_stir_bm_2022_hashid`

**Critical mappings**:
- `forms_indonesia` → `forms`
- `location_indonesia` → `region`
- `district_indonesia` → `sub_region`
- `"SubmissionDate"` → `submissiondate` (with transformation)

### From classroom_surveys_normalized.sql
The production model unpivots these columns (must exist):
- `s1`, `s2`, `s3`, `s4`
- `c1`, `c2`, `c3`
- `e1`, `e2`, `e3`
- `se1`, `se2`, `se3`, `se4`, `se5`
- `cc1`, `cc2`, `cc3`, `cc4`, `cc5`
- `gc1`, `gc2`, `gc3`, `gc4`, `gc5`
- `cro13ai`, `cro13aiii`, `cro13aiv`, `cro13av` (for derived fields)
- `cro13b`, `cro13c`

**Also requires**:
- `"KEY"` - Primary key
- `submissiondate` - For deduplication ordering
- `malepresent`, `femalepresent`
- `country`, `region`, `sub_region`
- `forms`, `program`, `plname`, `education_level`, `observation_term`, `meeting`, `role_coaching`

## Action Items

### 1. Resolve SubmissionDate Contradiction
**Check**: Does `SubmissionDate` exist in the actual `Indonesia_SCTO` source table?

**If YES** (SubmissionDate exists):
- Update `indonesia_normalized.sql` line 90 to:
  ```sql
  "SubmissionDate",  -- SubmissionDate exists in SurveyCTO Indonesia
  ```
- Remove the NULL cast

**If NO** (SubmissionDate doesn't exist):
- Update `int_classroom_surveys_indonesia.sql` line 56 comment to:
  ```sql
  -- Handle SubmissionDate (doesn't exist in SurveyCTO, NULL in Kobo)
  ```
- Consider using `starttime` or `date` as fallback for submissiondate

### 2. Verify All Expected Columns Exist
Run this query to check:
```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'new_staging'
  AND table_name = 'Indonesia_SCTO'
ORDER BY ordinal_position;
```

Compare with the 56 columns listed above.

### 3. Check for New Columns
If the source has NEW columns not in the model:
- Add them to `surveycto_data` CTE
- Add corresponding NULL or value in `kobo_data` CTE for UNION compatibility
- Update comment about column count (currently says "99 columns")

### 4. Verify Column Types
Ensure data types match expectations, especially:
- `_id` should be varchar/text (for KEY concatenation)
- `starttime`, `date`, `endtime` should be compatible with timestamp parsing
- All indicator columns (c1, e1, s1, etc.) should be compatible with bigint casting

### 5. Test KEY Generation
Verify KEY uniqueness:
```sql
SELECT "KEY", COUNT(*) as count
FROM intermediate.indonesia_normalized
GROUP BY "KEY"
HAVING COUNT(*) > 1;
```

Should return 0 rows (all KEYs should be unique).

## SQL Query to Check Source Schema

```sql
-- Get actual columns from source
SELECT 
    column_name,
    data_type,
    is_nullable,
    ordinal_position
FROM information_schema.columns
WHERE table_schema = 'new_staging'
  AND table_name = 'Indonesia_SCTO'
ORDER BY ordinal_position;

-- Check for SubmissionDate specifically
SELECT 
    CASE WHEN EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'new_staging' 
          AND table_name = 'Indonesia_SCTO' 
          AND column_name = 'SubmissionDate'
    ) THEN 'EXISTS' ELSE 'DOES NOT EXIST' END as submissiondate_status;

-- Count total columns
SELECT COUNT(*) as total_columns
FROM information_schema.columns
WHERE table_schema = 'new_staging'
  AND table_name = 'Indonesia_SCTO';
```

## Expected Column Count
- Comment says: "SurveyCTO has 99 columns"
- Model explicitly lists: 124 columns (including NULL casts for Kobo-only columns)
- Actual source may have: Different count (need to verify)

**Note**: The 99 columns likely refers to actual columns in the source table, while 124 includes NULL placeholders for UNION compatibility with Kobo.
