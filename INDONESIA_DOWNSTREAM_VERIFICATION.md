# Indonesia Downstream Dependencies Verification

## Data Flow Chain

```
Indonesia_SCTO + Indonesia_Kobo (sources)
    ↓
indonesia_normalized (intermediate/normalized)
    ↓
int_classroom_surveys_indonesia (intermediate/cleaned)
    ↓
classroom_surveys_merged (production/raw)
    ↓
classroom_surveys_normalized (production/derived)
    ↓
[All other production models]
```

## Verification Checklist

### ✅ 1. indonesia_normalized → int_classroom_surveys_indonesia

**Cleaned Model Requirements** (`int_classroom_surveys_indonesia.sql`):

#### Explicit Column References:
- ✅ `forms_indonesia` → maps to `forms` (line 7)
- ✅ `location_indonesia` → maps to `region` (line 9)
- ✅ `district_indonesia` → maps to `sub_region` (line 9)
- ✅ `SubmissionDate` → transforms to `submissiondate` (lines 57-64)
- ✅ `date`, `date_coaching` → transforms to `observation_date` (lines 26-31)
- ✅ `starttime` → transforms to `starttime` (lines 33-42)
- ✅ `endtime` → transforms to `endtime` (lines 44-53)
- ✅ All indicator columns: `c1`, `c2`, `c3`, `c1a`, `c2a`, `e1`, `e2`, `s1`, `s2`, `s3`, `s4`, `se1`, `se2`, `se3`
- ✅ All CRO columns: `cro1`-`cro12`, `cro7a`, `cro8a`, `cro13a`, `cro13av`, `cro13b`, `cro13c`
- ✅ Remarks columns: `remarks`, `remarks_classroom`, `remarks_coaching`

#### dbt_utils.star() Usage:
- Uses `dbt_utils.star()` with exceptions (line 8)
- Excludes: `programme`, `district_kota_kediri`, `location_indonesia`, `district_indonesia`, `s1`, `s2`, `s3`, `e1`, `e2`, `c1`, `c1a`, `c2`, `c2a`, `c3`, `se1`, `se2`, `se3`, `date`, `date_coaching`, `starttime`, `endtime`, `submissiondate`, `"CompletionDate"`, `_airbyte_indonesia_stir_bm_2022_hashid`

**Status**: ✅ All required columns present in `indonesia_normalized`

### ✅ 2. int_classroom_surveys_indonesia → classroom_surveys_merged

**Merged Model Requirements** (`classroom_surveys_merged.sql`):

- Uses `dbt_utils.union_relations()` (line 6)
- Automatically handles column alignment across all countries
- Only excludes: `_airbyte_emitted_at`, `_airbyte_normalized_at`
- Adds form mappings for Indonesia forms (lines 14, 18-21, 41, 43-44, 47, 49-50, 73, 75-76)

**Status**: ✅ No changes needed - union_relations handles column differences automatically

### ✅ 3. classroom_surveys_merged → classroom_surveys_normalized

**Normalized Model Requirements** (`classroom_surveys_normalized.sql`):

#### Columns Expected:
- ✅ `"KEY"` - Primary key
- ✅ `malepresent`, `femalepresent`
- ✅ `submissiondate`
- ✅ `observation_date`
- ✅ `remarks_qualitative`
- ✅ `country`, `region`, `sub_region`
- ✅ `program` (may be NULL for Indonesia)
- ✅ `forms`, `forms_verbose`, `forms_verbose_consolidated`
- ✅ `observation_term`
- ✅ `plname` (may be NULL for Indonesia)
- ✅ `education_level` (may be NULL for Indonesia)
- ✅ `meeting`, `role_coaching`
- ✅ All indicator columns: `s1`-`s4`, `c1`-`c3`, `e1`-`e3`, `se1`-`se5`, `cc1`-`cc5`, `gc1`-`gc5`
- ✅ CRO-13 fields: `cro13ai`, `cro13aiii`, `cro13aiv`, `cro13av`, `cro13b`, `cro13c`

**Potential Issues**:
- ⚠️ `cro13ai`, `cro13aiii`, `cro13aiv` - These are parsed from `cro13a` field in production model (space-separated values)
- ⚠️ `program` - Indonesia has `programme` (excluded in cleaned model), will be NULL
- ⚠️ `plname` - Not in Indonesia model, will be NULL
- ⚠️ `education_level` - Not in Indonesia model, will be NULL

**Status**: ✅ These NULLs are expected and handled by production model

### ✅ 4. All Production Models

**Common Requirements**:
- ✅ `"KEY"` - Present
- ✅ `submissiondate` - Present (from SubmissionDate transformation)
- ✅ `country` - Set to 'Indonesia'
- ✅ `region` - From `location_indonesia`
- ✅ `sub_region` - From `district_indonesia`
- ✅ `forms` - From `forms_indonesia`
- ✅ All indicator columns - Present
- ✅ All CRO columns - Present

**Status**: ✅ All production models should work correctly

## Changes Made That Affect Downstream

### ✅ Safe Changes (No Impact):
1. **KEY Generation**: Changed from `_id` to `"KEY"` column
   - Impact: None - KEY column still produced correctly
   - Downstream: Still receives `"KEY"` column as expected

2. **Added Columns**: `caseid`, `instanceID`, `formdef_id`, `formdef_version`, `review_quality`
   - Impact: None - These are passed through via `dbt_utils.star()`
   - Downstream: Will receive these columns (may be NULL for other countries via union)

3. **Kobo cro13a Variants**: Properly quoted column names
   - Impact: None - Aliases match expected names
   - Downstream: Receives correctly named columns

### ✅ No Breaking Changes:
- All column names remain the same
- All transformations remain the same
- All mappings remain the same
- Column order maintained for UNION ALL compatibility

## Potential Issues to Monitor

### 1. Airbyte Columns
**Issue**: Model includes `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta`
- **Impact**: If these don't exist in source, model will fail
- **Solution**: If error occurs, remove these columns or handle as NULL

### 2. Missing Columns in Source
**Issue**: If any expected column doesn't exist in refreshed source
- **Impact**: Model compilation/run will fail with clear error
- **Solution**: Add column to source or cast as NULL in model

### 3. NULL Values
**Issue**: Some columns may be NULL (program, plname, education_level)
- **Impact**: Expected behavior - handled by production models
- **Solution**: None needed - this is expected

## Testing Recommendations

### 1. Test Cleaned Model
```bash
dbt run --select int_classroom_surveys_indonesia
dbt test --select int_classroom_surveys_indonesia
```

### 2. Test Merged Model
```bash
dbt run --select classroom_surveys_merged
dbt test --select classroom_surveys_merged
```

### 3. Test Production Models
```bash
dbt run --select classroom_surveys_normalized+
dbt test --select classroom_surveys_normalized+
```

## Summary

✅ **All downstream dependencies are intact**
✅ **No breaking changes to intermediate → production logic**
✅ **All required columns present**
✅ **Column names unchanged**
✅ **Transformations unchanged**
✅ **Mappings unchanged**

**Status**: Ready for production testing
