# Uganda Column Structure Comparison

## Uganda_SCTO Columns (Actual Source)

1. SubmissionDate ✓
2. starttime
3. endtime
4. deviceid
5. devicephonenum
6. username
7. device_info
8. duration
9. caseid
10. plname
11. location_uganda
12. education_level
13. district_central, district_acholi, district_lango, district_westnile, district_karamoja, district_teso, district_mbale, district_busoga, district_masaka, district_bunyoro, district_rwenzori, district_ankole, district_kigezi
14. forms_uganda
15. observation_term
16. meeting
17. expected
18. malepresent
19. femalepresent
20. date
21. facilitator_role
22. facilitator_gender
23. facilitator_others
24. observer_role
25. observer_gender
26. observer_others
27. teacher_gender
28. teacher_others
29. s1, s2, s3, s4
30. e1, e2
31. c1, c1a, c2, c2a, c3
32. se1, se2, se3, se4, se5
33. remarks
34. meeting_coaching
35. date_coaching
36. duration_coaching
37. facilitator_role_coaching
38. name_of_the_coach
39. role_coaching
40. name_of_the_coachee
41. coach_gender
42. coach_gender_specify
43. coachee_gender
44. coachee_gender_specify
45. cc1, cc2, cc3, cc4, cc5
46. remarks_coaching
47. cro1, cro2, cro3, cro4, cro5
48. cro7, cro7a, cro8, cro8a, cro9
49. cro10, cro11, cro12
50. cro13a
51. cro13aiii
52. cro13b, cro13c
53. remarks_classroom
54. instanceID
55. formdef_version
56. formdef_id
57. review_quality
58. KEY
59. @ (special character column?)
60. Location
61. program
62. si_districts

## Uganda_Kobo Columns (Actual Source)

1. starttime
2. endtime
3. deviceid
4. devicephonenum
5. username
6. device_info
7. duration
8. plname
9. program
10. location_uganda
11. education_level
12. district_central, district_acholi, district_lango, district_westnile, district_karamoja, district_teso, district_mbale, district_busoga, district_masaka, district_bunyoro, district_rwenzori, district_ankole, district_kigezi
13. si_districts
14. forms_uganda
15. Location
16. _Location_latitude
17. _Location_longitude
18. _Location_altitude
19. _Location_precision
20. observation_term
21. meeting
22. n_seca
23. expected
24. malepresent
25. femalepresent
26. date
27. facilitator_role
28. facilitator_gender
29. facilitator_others
30. observer_role
31. observer_gender
32. observer_others
33. teacher_gender
34. teacher_others
35. n1, s1, s2, s3, s4, n2
36. e1, e2, n3
37. c1, c1a, c2, c2a, c3, n4
38. se1, se2, se3, se4, se5
39. remarks
40. n_secc
41. date_coaching
42. duration_coaching
43. facilitator_role_coaching
44. name_of_the_coach
45. role_coaching
46. name_of_the_coachee
47. coach_gender
48. coach_gender_specify
49. coachee_gender
50. coachee_gender_specify
51. n1_secd
52. cc1, cc2, cc3, cc4, cc5
53. remarks_coaching
54. n1_sece
55. cro1, cro2, cro3
56. n2_sece
57. cro4, cro5
58. n3_sece
59. cro7, cro7a, cro8, cro8a, cro9
60. n4_sece
61. cro10, cro11, cro12
62. n5_sece
63. cro13a
64. cro13aiii
65. cro13aiii/growth_mindset (and many other cro13aiii variants with slashes)
66. cro13b
67. cro13c
68. remarks_classroom
69. _id
70. _uuid
71. _submission_time
72. _validation_status
73. _notes
74. _status
75. _submitted_by
76. __version__
77. _tags
78. _index

## Key Differences

### Columns ONLY in SurveyCTO:
- SubmissionDate ✓
- caseid
- instanceID
- formdef_version
- formdef_id
- review_quality
- KEY (but Kobo has _id)
- @ (special character column - may need quoting)
- si_districts (also in Kobo)

### Columns ONLY in Kobo:
- device_det (not listed but may exist)
- n_seca, n_secc, n1, n2, n3, n4, n1_secd, n1_sece, n2_sece, n3_sece, n4_sece, n5_sece
- _Location_latitude, _Location_longitude, _Location_altitude, _Location_precision
- cro13aiii/growth_mindset and many other cro13aiii variants with slashes
- _id, _uuid, _submission_time, _validation_status, _notes, _status, _submitted_by, __version__, _tags, _index

### Columns in BOTH:
- Most core columns are shared
- Both have cro13aiii (but Kobo has variants with slashes)
- Both have Location column
- Both have program, plname, education_level (Indonesia didn't have these!)

## Issues to Address

1. **KEY Generation**: SurveyCTO has KEY column, NOT _id
2. **Missing SurveyCTO Columns**: caseid, instanceID, formdef_version, formdef_id, review_quality, SubmissionDate
3. **Kobo cro13aiii Variants**: Have slashes in names (cro13aiii/growth_mindset, etc.) - need proper quoting
4. **@ Column**: Special character column in SurveyCTO - need proper quoting
5. **Location Columns**: Kobo has _Location_* columns that need to be included
6. **cro13ai and cro13aiv**: Need to check if Uganda has these (Indonesia didn't)
