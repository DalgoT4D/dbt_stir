# Fix: Indonesia Data Flow to classroom_surveys_normalized

## Issue Identified

The `classroom_surveys_normalized` production model expects these columns from `classroom_surveys_merged`:
- `cro13ai` - Used to parse space-separated values (line 12-24)
- `cro13aiii` - Used to parse space-separated values (line 27-33)
- `cro13aiv` - Used to parse space-separated values (line 36-41)

**Problem**: Indonesia only has `cro13a` and `cro13av` columns, NOT `cro13ai`, `cro13aiii`, or `cro13aiv`.

**Impact**: If these columns don't exist, the production model will fail with "column does not exist" error when trying to parse them.

## Fix Applied

Added the missing columns as NULL in both SurveyCTO and Kobo CTEs:

### SurveyCTO Section (Line 55-57):
```sql
-- cro13ai, cro13aiii, cro13aiv don't exist in Indonesia (only in some other countries)
CAST(NULL AS varchar) AS cro13ai,
CAST(NULL AS varchar) AS cro13aiii,
CAST(NULL AS varchar) AS cro13aiv,
```

### Kobo Section (Line 150-152):
```sql
-- cro13ai, cro13aiii, cro13aiv don't exist in Indonesia Kobo (only in some other countries)
CAST(NULL AS varchar) AS cro13ai,
CAST(NULL AS varchar) AS cro13aiii,
CAST(NULL AS varchar) AS cro13aiv,
```

## Result

✅ **Columns now exist** in `indonesia_normalized` output
✅ **Will flow through** `int_classroom_surveys_indonesia` via `dbt_utils.star()`
✅ **Will flow through** `classroom_surveys_merged` via `dbt_utils.union_relations()`
✅ **Will be available** in `classroom_surveys_normalized` for parsing

## Expected Behavior

In `classroom_surveys_normalized`:
- `cro13ai` will be NULL for Indonesia records
- `cro13aiii` will be NULL for Indonesia records  
- `cro13aiv` will be NULL for Indonesia records
- All derived indicators (`cro13ai_behavior_engagement_derived`, etc.) will be 0 (since NULL LIKE '%pattern%' evaluates to false)
- This is **expected behavior** - Indonesia doesn't have these fields, so they'll be NULL/0

## Verification

The production model uses:
```sql
CASE WHEN cro13ai LIKE '%behavior_engagement%' THEN 1 ELSE 0 END
```

With NULL values:
- `NULL LIKE '%pattern%'` → NULL
- `CASE WHEN NULL THEN 1 ELSE 0 END` → 0

So all derived indicators will be 0 for Indonesia, which is correct.

## Status

✅ **Fix Applied** - Indonesia data will now flow into `classroom_surveys_normalized` without errors
