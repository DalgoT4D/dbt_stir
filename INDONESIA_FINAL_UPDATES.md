# Indonesia Model - Final Updates for New Column Structure

## Summary of Changes Applied

### ✅ 1. Updated SurveyCTO Section
**Changed from NULL to actual columns:**
- `n1`, `n2`, `n3`, `n4` - Now included from source
- `_id` - Now included from source (column 14)
- `_tags`, `_uuid` - Now included from source
- `_index`, `_notes` - Now included from source
- `n_seca`, `n_secc`, `_status` - Now included from source
- `n1_secd`, `n1_sece`, `n2_sece`, `n2_secf`, `n3_sece`, `n4_sece`, `n5_sece` - Now included from source
- `date_cro` - Now included from source
- `device_det` - Now included from source
- `phonenumber` - Now included from source
- `_submitted_by` - Now included from source
- `interview_dur` - Now included from source
- `_submission_time` - Now included from source
- `_validation_status` - Now included from source
- All cro13a variant columns (with underscores) - Now included from source

### ✅ 2. Updated KEY Generation
- Now uses `_id` first (if exists), then `KEY` as fallback
- Updated row numbering to check `_id` first

### ✅ 3. Updated Kobo Section
- Removed quotes and AS aliases from cro13a variant columns
- Columns now use underscores directly (matching SurveyCTO structure)
- Example: Changed from `"cro13a/digital_learning" AS cro13a_digital_learning` to `cro13a_digital_learning`

### ✅ 4. Updated Column Count Comment
- Changed from "~59 columns" to "~131 columns" for SurveyCTO
- Changed from "~91 columns" to "~131 columns" for Kobo
- Both tables now aligned

## Column Differences Remaining

### SurveyCTO Only:
- `caseid`
- `instanceID`
- `formdef_id`
- `formdef_version`
- `review_quality`
- `"SubmissionDate"`

### Kobo Only:
- `start`
- `"end"` (reserved word)
- `__version__`

## Status

✅ **All updates complete** - Model now matches new column structures (131 columns each)

Both sources are now much more aligned, with only 7 columns different between them.
