# Indonesia New Column Structure Analysis

## Key Changes Identified

### Indonesia_SCTO - Now Has Columns That Were Previously Missing:
- ✅ `n1`, `n2`, `n3`, `n4` - Now exist (were NULL before)
- ✅ `_tags`, `_uuid` - Now exist (were NULL before)
- ✅ `start` - Now exists (was NULL before)
- ✅ `_index`, `_notes` - Now exist (were NULL before)
- ✅ `n_seca`, `n_secc`, `_status` - Now exist (were NULL before)
- ✅ `n1_secd`, `n1_sece`, `n2_sece`, `n2_secf`, `n3_sece`, `n4_sece`, `n5_sece` - Now exist (were NULL before)
- ✅ `date_cro` - Now exists (was NULL before)
- ✅ `device_det` - Now exists (was NULL before)
- ✅ `phonenumber` - Now exists (was NULL before)
- ✅ `_submitted_by` - Now exists (was NULL before)
- ✅ `interview_dur` - Now exists (was NULL before)
- ✅ `_submission_time` - Now exists (was NULL before)
- ✅ `_validation_status` - Now exists (was NULL before)
- ✅ All cro13a variant columns with underscores (not slashes):
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

### Indonesia_Kobo - Column Names Changed:
- ❌ Columns with slashes (`cro13a/digital_learning`) → Now have underscores (`cro13a_digital_learning`)
- ✅ All columns now use underscores, not slashes

## Impact on Model

### SurveyCTO Section Changes Needed:
1. Remove NULL casts for columns that now exist
2. Include actual columns instead of NULL

### Kobo Section Changes Needed:
1. Change from quoted slashes (`"cro13a/digital_learning"`) to regular column names (`cro13a_digital_learning`)
2. Remove AS aliases (columns already have correct names)
