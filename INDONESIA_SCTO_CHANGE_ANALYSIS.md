# Indonesia_SCTO Source Data Change - Impact Analysis

## Overview
This document analyzes the implications of changes to the `Indonesia_SCTO` source table in the dbt project. The source is defined in `models/_surveycto__sources.yml` and flows through multiple transformation layers.

## Source Definition
- **Source Name**: `source_classroom_surveys.indonesia`
- **Database Table**: `new_staging.Indonesia_SCTO`
- **Description**: One record per form data

## Data Flow Architecture

```
Indonesia_SCTO (source)
    ↓
indonesia_normalized (intermediate/normalized)
    ↓
int_classroom_surveys_indonesia (intermediate/cleaned)
    ↓
classroom_surveys_merged (production/raw)
    ↓
[Multiple derived production models]
```

## Critical Columns Expected from Indonesia_SCTO

Based on `models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql`, the following columns are **explicitly referenced** from the SurveyCTO source:

### Core Identification Columns
- `_id` - Used for creating KEY column (critical for deduplication)
- `deviceid` - Used as fallback for row numbering
- `starttime` - Used for row numbering and timestamp parsing
- `date` - Used for row numbering and date parsing

### Survey Response Columns (Required)
- `c1`, `c2`, `c3` - Critical thinking indicators
- `c1a`, `c2a` - Additional critical thinking fields
- `e1`, `e2` - Engagement indicators
- `s1`, `s2`, `s3`, `s4` - Safety indicators
- `se1`, `se2`, `se3`, `se4`, `se5` - Self-esteem indicators
- `cc1`, `cc2`, `cc3`, `cc4`, `cc5` - Coaching call fields
- `gc1`, `gc2`, `gc3`, `gc4`, `gc5` - Group coaching fields
- `cro1`, `cro2`, `cro3`, `cro4`, `cro5`, `cro7`, `cro8`, `cro9` - Classroom observation fields
- `cro10`, `cro11`, `cro12`, `cro7a`, `cro8a` - Additional classroom observation fields
- `cro13a`, `cro13b`, `cro13c`, `cro13av` - Extended classroom observation fields

### Metadata Columns
- `ge_1`, `ge_2`, `ge_3`, `ge_4`, `ge_5` - Gender equity fields
- `endtime` - End timestamp
- `meeting` - Meeting identifier
- `remarks` - Qualitative remarks
- `deviceid` - Device identifier
- `duration` - Duration field
- `expected` - Expected value
- `username` - Username
- `programme` - Programme identifier
- `device_info` - Device information
- `malepresent` - Male presence indicator
- `type_school` - School type
- `coach_gender` - Coach gender
- `date_coaching` - Coaching date
- `femalepresent` - Female presence indicator
- `observer_role` - Observer role
- `role_coaching` - Coaching role
- `coachee_gender` - Coachee gender
- `devicephonenum` - Device phone number
- `teacher_gender` - Teacher gender
- `teacher_others` - Other teacher info
- `forms_indonesia` - Form identifier (CRITICAL - used as `forms` in downstream)
- `observer_gender` - Observer gender
- `observer_others` - Other observer info
- `facilitator_role` - Facilitator role
- `meeting_coaching` - Meeting coaching field
- `observation_term` - Observation term
- `remarks_coaching` - Coaching remarks
- `duration_coaching` - Coaching duration
- `name_of_the_coach` - Coach name
- `remarks_classroom` - Classroom remarks
- `district_indonesia` - District identifier (CRITICAL - used as `sub_region` in downstream)
- `facilitator_gender` - Facilitator gender
- `facilitator_others` - Other facilitator info
- `location_indonesia` - Location identifier (CRITICAL - used as `region` in downstream)
- `name_of_the_coachee` - Coachee name
- `coach_gender_specify` - Coach gender specification
- `coachee_gender_specify` - Coachee gender specification
- `remarks_group_coaching` - Group coaching remarks
- `facilitator_role_coaching` - Facilitator role in coaching

### Airbyte Columns (if using Airbyte)
- `_airbyte_raw_id`
- `_airbyte_extracted_at`
- `_airbyte_meta`

## Key Processing Logic

### 1. KEY Generation (Critical for Deduplication)
```sql
COALESCE(
    CASE WHEN _id IS NOT NULL AND btrim(_id) != '' THEN CONCAT('scto_', _id) ELSE NULL END,
    CONCAT('scto_', CAST(row_num AS varchar))
) AS "KEY"
```
**Impact**: If `_id` column changes or is removed, KEY generation will fall back to row numbers, which may cause deduplication issues.

### 2. Row Numbering Fallback
```sql
ROW_NUMBER() OVER (ORDER BY COALESCE(_id, deviceid, starttime, date)) AS row_num
```
**Impact**: If multiple of these columns change, row ordering may change, affecting KEY generation.

### 3. Column Mapping in Cleaned Model
The `int_classroom_surveys_indonesia` model maps:
- `forms_indonesia` → `forms`
- `location_indonesia` → `region`
- `district_indonesia` → `sub_region`

**Impact**: If these column names change, downstream models will break.

## Downstream Dependencies

### Direct Dependencies
1. **indonesia_normalized** - Combines SurveyCTO and Kobo data
   - Uses explicit column list (99 columns expected from SurveyCTO)
   - Creates unified schema with Kobo data (131 columns)

2. **int_classroom_surveys_indonesia** - Cleans and transforms normalized data
   - Uses `dbt_utils.star()` with exceptions
   - Maps specific columns for region/sub_region
   - Transforms date/timestamp fields

3. **classroom_surveys_merged** - Merges all country data
   - Uses `dbt_utils.union_relations()` to combine 6 countries
   - Maps Indonesia-specific form codes (gc_indo, nm_indo, cc_indo, cro_indo, etc.)

### Production Models (Indirect Dependencies)
All production models in `models/production/classroom_surveys/derived/` depend on `classroom_surveys_merged`, including:
- `activity_overview.sql`
- `behavioral_*.sql` (multiple files)
- `classroom_observations.sql`
- `coaching_calls.sql`
- `curiosity_critical_thinking_*.sql` (multiple files)
- `engagement_*.sql` (multiple files)
- `safety_*.sql` (multiple files)
- `self_esteem_*.sql` (multiple files)
- And many more...

## Potential Breaking Points

### 1. Missing Columns
If any of the explicitly referenced columns are removed or renamed:
- **Immediate Failure**: SQL compilation error in `indonesia_normalized.sql`
- **Affected Models**: All downstream models

### 2. Column Type Changes
If column types change:
- **Potential Issues**: Type casting errors, data truncation
- **Example**: If `_id` changes from varchar to integer, KEY concatenation may fail

### 3. NULL Value Changes
If columns that were previously non-null become nullable:
- **Impact**: May affect COALESCE logic and downstream calculations
- **Example**: If `forms_indonesia` becomes NULL, the `forms` field downstream will be NULL

### 4. Data Quality Changes
- **KEY Generation**: If `_id` values become NULL or blank, fallback to row numbers may create duplicate KEYs
- **Deduplication**: Changes to timestamp fields (`starttime`, `date`, `_submission_time`) may affect deduplication logic

### 5. Schema Tests
The following tests are defined for Indonesia models:
- `not_null` on `submissiondate` (currently failing with 722 NULLs)
- `not_null` on `region` 
- Type tests for `submissiondate` (must be `timestamp with time zone`)
- Type tests for `region` (must be `text`)
- `not_null` and `unique` on `"KEY"` in `indonesia_normalized`

## Recommended Actions

### 1. Immediate Checks
- [ ] Verify all 99 columns still exist in the source table
- [ ] Check column data types match expectations
- [ ] Verify `_id` column still exists and contains valid values
- [ ] Check `forms_indonesia`, `location_indonesia`, `district_indonesia` still exist

### 2. Schema Comparison
Run a schema comparison query to identify:
- New columns added
- Columns removed
- Columns renamed
- Data type changes

### 3. Data Quality Checks
- [ ] Check NULL rates for critical columns (`_id`, `forms_indonesia`, etc.)
- [ ] Verify KEY uniqueness (should be unique per record)
- [ ] Check date/timestamp format consistency

### 4. Testing Strategy
After identifying changes:
1. Update `indonesia_normalized.sql` if columns changed
2. Update `int_classroom_surveys_indonesia.sql` if mapping logic needs changes
3. Run `dbt test` to verify all tests pass
4. Check row counts match expectations
5. Validate downstream models still work

### 5. Documentation Updates
- Update column comments in `indonesia_normalized.sql` if structure changed
- Update this analysis document with actual changes found

## SQL Queries for Investigation

### Check Current Schema
```sql
SELECT 
    column_name,
    data_type,
    is_nullable,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'new_staging'
  AND table_name = 'Indonesia_SCTO'
ORDER BY ordinal_position;
```

### Check Row Counts
```sql
SELECT COUNT(*) as row_count
FROM new_staging."Indonesia_SCTO";
```

### Check KEY Uniqueness (after running normalized model)
```sql
SELECT "KEY", COUNT(*) as count
FROM intermediate.indonesia_normalized
GROUP BY "KEY"
HAVING COUNT(*) > 1;
```

### Check Critical Column NULL Rates
```sql
SELECT 
    COUNT(*) as total_rows,
    COUNT(_id) as _id_not_null,
    COUNT(forms_indonesia) as forms_not_null,
    COUNT(location_indonesia) as location_not_null,
    COUNT(district_indonesia) as district_not_null,
    COUNT(starttime) as starttime_not_null,
    COUNT(date) as date_not_null
FROM new_staging."Indonesia_SCTO";
```

## Files That May Need Updates

1. **models/intermediate/classroom_surveys/normalized/indonesia_normalized.sql**
   - Update column list if columns added/removed/renamed
   - Update comments about column counts (currently says "99 columns")

2. **models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_indonesia.sql**
   - Update column mappings if source columns changed
   - Update dbt_utils.star() exceptions if needed

3. **models/_surveycto__sources.yml**
   - Update description if structure significantly changed

4. **models/intermediate/classroom_surveys/schema.yml**
   - Update tests if data quality expectations changed

## Notes
- The model currently expects SurveyCTO to have 99 columns (per comment in code)
- SurveyCTO data is UNION ALL'd with Kobo data (131 columns)
- Deduplication happens based on KEY column
- The model handles missing columns by casting NULL values for Kobo-only columns
