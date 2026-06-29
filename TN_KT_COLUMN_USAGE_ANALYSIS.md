# Tamil Nadu and Karnataka Column Usage Analysis

## Key Findings

### Tamil Nadu Model Structure
- **`tamil_nadu_normalized.sql`**: Simple `SELECT *` - passes through ALL columns from source
- **`int_classroom_surveys_tamil_nadu.sql`**: Uses `dbt_utils.star()` to select all columns except those explicitly transformed

### Karnataka Model Structure  
- **`karnataka_normalized.sql`**: Simple `SELECT *` - passes through ALL columns from source
- **`int_classroom_surveys_karnataka.sql`**: Uses `dbt_utils.star()` to select all columns except those explicitly transformed

## Column Usage Comparison

### Fields Questioned (from Indonesia_SCTO):

| Field | Tamil Nadu | Karnataka | Notes |
|-------|-----------|-----------|-------|
| `n1`, `n2`, `n3`, `n4` | ❓ Unknown | ❓ Unknown | Would be passed through if they exist in source |
| `n_seca`, `n_secc` | ❓ Unknown | ❓ Unknown | Would be passed through if they exist |
| `n1_secd`, `n1_sece`, etc. | ❓ Unknown | ❓ Unknown | Would be passed through if they exist |
| `_id` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `_uuid` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `_tags` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `_index` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `_notes` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `_status` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `__version__` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `_submitted_by` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `_submission_time` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `_validation_status` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `phonenumber` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `device_det` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `interview_dur` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `date_cro` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `start` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `end` | ❓ Unknown | ❓ Unknown | Would be passed through if exists |
| `cro13a/*` variants | ❓ Unknown | ❓ Unknown | Would be passed through if they exist |
| `cro13av/*` variants | ❓ Unknown | ❓ Unknown | Would be passed through if they exist |

## How Tamil Nadu & Karnataka Process Data

### Tamil Nadu:
1. **Normalized**: `SELECT *` from source → All columns passed through
2. **Cleaned**: 
   - Uses `dbt_utils.star()` to select all columns EXCEPT:
     - `forms`, `district_tn`, `s1-s3`, `e1-e2`, `c1-c3`, `se1-se3`
     - `date`, `date_coaching`, `starttime`, `endtime`, `submissiondate`
     - `"CompletionDate"`, `_airbyte_tn_stir_bm_2022_hashid`
   - Explicitly transforms: `starttime`, `endtime`, `SubmissionDate` (simple casts)
   - **Result**: ALL other columns (including n*, _*, cro13a*, etc.) are passed through unchanged

### Karnataka:
1. **Normalized**: `SELECT *` from source → All columns passed through
2. **Cleaned**: 
   - Uses `dbt_utils.star()` to select all columns EXCEPT:
     - `forms`, `district_kt`, `date`, `date_coaching`
     - `starttime`, `endtime`, `submissiondate`
     - `"CompletionDate"`, `_airbyte_karnataka_stir_bm_2022_hashid`
   - Explicitly transforms: `starttime`, `endtime`, `SubmissionDate` (simple casts)
   - **Result**: ALL other columns (including n*, _*, cro13a*, etc.) are passed through unchanged

## Key Differences from Indonesia/Uganda

### Indonesia/Uganda Models:
- Have **explicit column selection** in normalized models
- Perform **UNION ALL** between SurveyCTO and Kobo sources
- Handle **column alignment** between two different sources
- Must explicitly list every column and cast NULL for missing ones

### Tamil Nadu/Karnataka Models:
- Use **`SELECT *`** in normalized models
- Have **single source** (no union)
- **No explicit column handling** - everything passes through
- Much simpler structure

## Conclusion

**For Tamil Nadu and Karnataka:**

### ✅ If these fields exist in the source tables:
- They are **automatically passed through** to downstream models
- They are **NOT used in any calculations** (same as Indonesia/Uganda)
- They are **available in final tables** via `dbt_utils.star()`

### ❌ If these fields don't exist in the source tables:
- They simply won't appear in the output
- No errors will occur (unlike Indonesia/Uganda which explicitly reference them)

### Impact of Removing from Source:
- **Tamil Nadu/Karnataka**: ✅ **No impact** - fields will simply disappear from output
- **Indonesia/Uganda**: ⚠️ **Requires model updates** - explicit column references need to be removed

## Recommendation

To determine if these fields exist in Tamil Nadu and Karnataka sources:

```sql
-- Check Tamil Nadu columns
SELECT column_name 
FROM information_schema.columns 
WHERE table_schema = 'new_staging' 
  AND table_name = 'TN_Data'
  AND column_name IN ('n1', 'n2', 'n3', 'n4', '_id', '_uuid', '_tags', 
                      '_index', '_notes', '_status', '__version__', 
                      '_submitted_by', '_submission_time', '_validation_status',
                      'phonenumber', 'device_det', 'interview_dur', 'date_cro')
ORDER BY column_name;

-- Check Karnataka columns
SELECT column_name 
FROM information_schema.columns 
WHERE table_schema = 'new_staging' 
  AND table_name = 'KT_Data'
  AND column_name IN ('n1', 'n2', 'n3', 'n4', '_id', '_uuid', '_tags', 
                      '_index', '_notes', '_status', '__version__', 
                      '_submitted_by', '_submission_time', '_validation_status',
                      'phonenumber', 'device_det', 'interview_dur', 'date_cro')
ORDER BY column_name;
```

**Most likely**: Tamil Nadu and Karnataka sources **do NOT have** these fields, as they appear to be:
- Kobo-specific metadata fields (`_id`, `_uuid`, `_tags`, `_index`, `_notes`, `_status`, `__version__`, `_submitted_by`, `_submission_time`, `_validation_status`)
- New section markers added to Indonesia/Uganda (`n1-n4`, `n_seca`, `n_secc`, `n*_sec*`)
- Device metadata (`phonenumber`, `device_det`, `interview_dur`, `date_cro`)
- Teaching strategy variants (`cro13a/*`, `cro13av/*`)

These fields were likely added when Indonesia and Uganda transitioned to using Kobo alongside SurveyCTO.
