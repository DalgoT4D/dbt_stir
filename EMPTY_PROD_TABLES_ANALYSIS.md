# Empty Production Tables – Analysis

## Tables you mentioned (all empty)

| Prod table | dbt model | Source (ref) | Main filters |
|------------|----------|--------------|--------------|
| **engagement_students** | `engagement_students.sql` | `classroom_surveys_normalized` | `behavior = 'Engagement'`, `forms IN ('cro_ug','cro','cro_indo')`, `subindicator IN ('e1','e2','e3')`, `HAVING region IS NOT NULL AND sub_region IS NOT NULL` |
| **engagement_teachers** | `engagement_teachers.sql` | `classroom_surveys_normalized` | `behavior = 'Engagement'`, `forms IN ('nm_indo','nm_art','nm','nm_ug','nm_coart')`, `subindicator IN ('e1','e2','e3')`, `HAVING region IS NOT NULL AND sub_region IS NOT NULL` |
| **engagement_trendline** | `engagement_trendline.sql` | `classroom_surveys_normalized` | `behavior IN ('Engagement')` only |

All three read from **`classroom_surveys_normalized`** (which is built from `classroom_surveys_merged` → union of the six cleaned models: Uganda, Indonesia, Delhi, Tamil Nadu, Karnataka, Ethiopia).

---

## Root cause (fixed)

**Issue:** In `classroom_surveys_normalized`, **all** score columns (s1–s4, c1–c3, e1–e3, se1–se5, cc1–cc5, gc1–gc5, ad1–ad9, sr1–sr6) were extracted with a **strict regex** `'^-?\\d+$'` (integers only, no spaces or decimals). Source data often has values like `" 1 "`, `"1.0"`, or other formats that did not match, so the UNNEST produced `NULL` for those positions. The `classified` CTE drops all rows with `WHERE score IS NOT NULL`, so every behavior row (Safety, Engagement, Curiosity & Critical Thinking, Self Esteem, Intentional Teaching, etc.) was dropped when scores didn’t match the regex, leaving many prod tables empty.

**Fix (applied):** In `classroom_surveys_normalized.sql`, **every** score column in the UNNEST was updated to:
- **Trim** with `btrim(coalesce(col::text, ''))` so leading/trailing spaces are allowed.
- **More permissive pattern** `'^-?\d+\.?\d*$'` so optional decimals (e.g. `1.0`) are accepted.
- **Cast** via `(btrim(col::text)::numeric)::bigint` so values are stored as bigint for the unpivot.

**Before → After (row counts):**

| Model | Before | After |
|-------|--------|-------|
| engagement_students | 0 | 1,500 |
| engagement_teachers | 0 | 1,758 |
| engagement_trendline | 0 | 6,689 |
| safety_students | 0 | 1,500 |
| safety_teachers | 0 | 1,731 |
| safety_trendline | 0 | 6,631 |
| safety_officials | 0 | 4,589 |
| curiosity_critical_thinking_students | 0 | 1,500 |
| curiosity_critical_thinking_teachers | 0 | 1,719 |
| curiosity_critical_thinking_officials | 0 | 2,751 |
| self_esteem_students | 0 | 1,500 |
| self_esteem_teachers | 0 | 1,786 |
| self_esteem_trendline | 0 | 6,668 |
| self_esteem_officials | 0 | 2,882 |
| intentional_teaching_dc | 0 | 2,641 |
| intentional_teaching_elm | 0 | 225 |
| intentional_teaching_gc | 0 | 330 |
| intentional_teaching_officials | 0 | 33,209 |
| intentional_teching_dc_overall | 0 | 2,641 |
| intentional_teching_elm_overall | 0 | 225 |
| behavioral_overview_trendline | 0 | 6,580 |
| c_ct_behavioral | 0 | 3,969 |

**Models they refer to:** All of these read from **`classroom_surveys_normalized`** (which reads from `classroom_surveys_merged` → union of the six cleaned models). Data was present through merged; the loss was in the score extraction step in `classroom_surveys_normalized`.

---

## Other prod models that can be empty (same pattern)

Same dependency on `classroom_surveys_normalized` + behavior + forms + often `HAVING region IS NOT NULL AND sub_region IS NOT NULL`:

| Model | Behavior / focus | Forms filter | Same “can be empty” pattern |
|-------|-------------------|--------------|-----------------------------|
| **safety_students** | Safety | cro_ug, cro, cro_indo | Yes |
| **safety_teachers** | Safety | nm_indo, nm_art, nm, nm_ug, nm_coart | Yes |
| **safety_trendline** | Safety | (none) | Yes |
| **curiosity_critical_thinking_students** | Curiosity & Critical Thinking | cro_ug, cro, cro_indo | Yes |
| **curiosity_critical_thinking_teachers** | Curiosity & Critical Thinking | nm_indo, nm_art, nm, nm_ug, nm_coart | Yes |
| **curiosity_critical_thinking_officials** | Curiosity & Critical Thinking | dmpc, dam, … | Yes |
| **self_esteem_students** | Self Esteem | cro_ug, cro, cro_indo | Yes |
| **self_esteem_teachers** | Self Esteem | nm_indo, nm_art, nm, nm_ug, nm_coart | Yes |
| **self_esteem_officials** | Self Esteem | dmpc, dam, … | Yes |
| **self_esteem_trendline** | Self Esteem | (none) | Yes |
| **engagement_officials** | Engagement | dmpc, dam, … | Yes |
| **intentional_teaching_*** | Intentional Teaching | various | Yes |
| **behavioral_officials** | Aggregated counts | various | Yes |
| **behavioral_students** | Aggregated counts | cro_ug, cro, cro_indo | Yes |
| **behavioral_teachers** | Aggregated counts | nm_indo, nm_art, nm, nm_ug, nm_coart | Yes |
| **behavioral_trendline** | Aggregated | (none) but HAVING region/sub_region | Yes |
| **behavioral_overview** | Multiple behaviors | (none) but HAVING region/sub_region | Yes |
| **behavioral_overview_trendline** | Multiple behaviors | (none) | Yes |
| **c_ct_behavioral** | Curiosity & Critical Thinking | (filter in model) | Yes |

So: **any prod table that is a direct or aggregated select from `classroom_surveys_normalized` with behavior/forms/region filters can be empty** if there is no matching data (or if `region`/`sub_region` are null and the model uses `HAVING region IS NOT NULL AND sub_region IS NOT NULL`).

---

## Summary

- All previously empty prod tables read from **`classroom_surveys_normalized`** and depend on score columns (s1–s4, c1–c3, e1–e3, se1–se5, cc1–cc5, gc1–gc5, ad*, sr*) being parsed correctly in the UNNEST.
- They were empty because the **strict regex** used for score extraction rejected values with spaces or decimals, so scores became NULL and the `classified` CTE dropped those rows.
- **Fix:** The same permissive extraction (btrim + pattern `'^-?\d+\.?\d*$'` + numeric::bigint) was applied to **all** score columns in `classroom_surveys_normalized.sql`. No prod tables remain empty due to this issue.
