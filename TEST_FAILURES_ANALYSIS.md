# Test Failures Analysis

## Summary
The dbt tests are failing due to two main issues:

### 1. Type Mismatches

#### String Columns (forms, region, sub_region, country)
- **Expected**: `text`
- **Actual**: Likely `varchar` or `character varying`
- **Affected Tests**: All `dbt_expectations_expect_column_values_to_be_of_type_*_forms__text`, `*_region__text`, `*_sub_region__text`
- **Solution**: Cast columns explicitly to `text` in SQL models

#### Timestamp Columns (submissiondate)
- **Expected**: `timestamp with time zone`
- **Actual**: `timestamp` (without time zone)
- **Affected Tests**: 
  - `dbt_expectations_expect_column_values_to_be_of_type_int_classroom_surveys_delhi_submissiondate__timestamp_with_time_zone` (FAIL 2)
  - `dbt_expectations_expect_column_values_to_be_of_type_int_classroom_surveys_ethiopia_submissiondate__timestamp_with_time_zone` (FAIL 1)
  - `dbt_expectations_expect_column_values_to_be_of_type_classroom_surveys_merged_submissiondate__timestamp_with_time_zone` (FAIL 1)
- **Solution**: Cast to `timestamp with time zone` using `::timestamptz` or `AT TIME ZONE`

### 2. NULL Values

#### submissiondate Columns
- **Expected**: NOT NULL
- **Actual**: Contains NULL values
- **Affected Tests**:
  - `not_null_activity_overview_submissiondate` (FAIL 9740)
  - `not_null_behavioral_officials_submissiondate` (FAIL 8829)
  - `not_null_behavioral_students_submissiondate` (FAIL 247108)
  - `not_null_behavioral_teachers_submissiondate` (FAIL 8829)
  - `not_null_behavioral_trendline_submissiondate` (FAIL 8829)
  - `not_null_classroom_surveys_merged_submissiondate` (FAIL 9734)
  - `not_null_classroom_surveys_normalized_submissiondate` (FAIL 272564)
  - `not_null_int_classroom_surveys_indonesia_submissiondate` (FAIL 722)
  - `not_null_int_classroom_surveys_uganda_submissiondate` (FAIL 9012)
- **Solution**: Either filter out NULLs in models or remove `not_null` constraint from schema.yml

#### region Columns
- **Expected**: NOT NULL
- **Actual**: Contains NULL values
- **Affected Tests**:
  - `not_null_activity_overview_region` (FAIL 93)
- **Solution**: Filter out NULLs or remove constraint

## Files That Need Fixes

### Intermediate Models (Type Casting)
1. `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_delhi.sql`
   - Cast `forms`, `country`, `region`, `sub_region` to `text`
   - Cast `submissiondate` to `timestamp with time zone`

2. `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_tamil_nadu.sql`
   - Cast `forms`, `country`, `region`, `sub_region` to `text`
   - Cast `submissiondate` to `timestamp with time zone`

3. `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_indonesia.sql`
   - Cast `forms`, `country`, `region`, `sub_region` to `text`
   - Cast `submissiondate` to `timestamp with time zone`

4. `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_uganda.sql`
   - Cast `forms`, `country`, `region`, `sub_region` to `text`
   - Ensure `submissiondate` is `timestamp with time zone`

5. `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_karnataka.sql`
   - Cast `forms`, `country`, `region`, `sub_region` to `text`
   - Ensure `submissiondate` is `timestamp with time zone`

6. `models/intermediate/classroom_surveys/cleaned/int_classroom_surveys_ethiopia.sql`
   - Cast `forms`, `country`, `region`, `sub_region` to `text`
   - Ensure `submissiondate` is `timestamp with time zone`

### Production Models
- May need to cast columns to `text` if they're selecting from intermediate models
- Ensure `submissiondate` is properly cast throughout the pipeline

### Schema Files
- Consider removing `not_null` constraints for `submissiondate` if NULLs are expected
- Or add filters to remove NULLs in the models
