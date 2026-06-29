# Indonesia Updated Column Structure - Changes Applied

## Major Changes Identified

### 1. SurveyCTO Now Has Many Columns That Were Previously Missing:
- ✅ `n1`, `n2`, `n3`, `n4` - Now exist (were NULL before)
- ✅ `_id` - Now exists (column 14)
- ✅ `_tags`, `_uuid` - Now exist
- ✅ `start` - Still doesn't exist (Kobo only)
- ✅ `_index`, `_notes` - Now exist
- ✅ `n_seca`, `n_secc`, `_status` - Now exist
- ✅ `n1_secd`, `n1_sece`, `n2_sece`, `n2_secf`, `n3_sece`, `n4_sece`, `n5_sece` - Now exist
- ✅ `date_cro` - Now exists
- ✅ `device_det` - Now exists
- ✅ `phonenumber` - Now exists
- ✅ `_submitted_by` - Now exists
- ✅ `interview_dur` - Now exists
- ✅ `_submission_time` - Now exists
- ✅ `_validation_status` - Now exists
- ✅ All cro13a variant columns with underscores:
  - `cro13a_digital_learning`
  - `cro13av_digital_learning`
  - `cro13a_settlers___stirrers`
  - `cro13a_pairwork___groupwork`
  - `cro13av_settlers___stirrers`
  - `cro13av_pairwork___groupwork`
  - `cro13a_collab___coop_learning`
  - `cro13av_collab___coop_learning`
  - `cro13a_differentiated_instruction`
  - `cro13av_differentiated_instruction`

### 2. Kobo Column Names Changed:
- ❌ Columns with slashes (`cro13a/digital_learning`) → Now have underscores (`cro13a_digital_learning`)
- ✅ All columns now use underscores, matching SurveyCTO structure

## Changes Applied to Model

### SurveyCTO Section:
1. ✅ Changed `n1-n4` from NULL to actual columns
2. ✅ Changed `_id` from NULL to actual column (now exists)
3. ✅ Updated KEY generation to use `_id` first, then `KEY` as fallback
4. ✅ Changed `_tags`, `_uuid` from NULL to actual columns
5. ✅ Changed `_index`, `_notes` from NULL to actual columns
6. ✅ Changed `n_seca`, `n_secc`, `_status` from NULL to actual columns
7. ✅ Changed `n1_secd-n5_sece` from NULL to actual columns
8. ✅ Changed `date_cro` from NULL to actual column
9. ✅ Changed `device_det` from NULL to actual column
10. ✅ Changed `phonenumber` from NULL to actual column
11. ✅ Changed `_submitted_by` from NULL to actual column
12. ✅ Changed `interview_dur` from NULL to actual column
13. ✅ Changed `_submission_time` from NULL to actual column
14. ✅ Changed `_validation_status` from NULL to actual column
15. ✅ Changed all cro13a variant columns from NULL to actual columns

### Kobo Section:
1. ✅ Removed quotes and AS aliases from cro13a variant columns (now use underscores directly)
2. ✅ Columns now match SurveyCTO naming (underscores, not slashes)

## Column Count Update
- Updated comment: Both tables now have ~131 columns (aligned)

## Status
✅ **All updates applied** - Model now matches new column structures
