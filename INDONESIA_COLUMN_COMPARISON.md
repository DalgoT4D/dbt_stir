# Indonesia Column Structure Comparison

## Indonesia_SCTO Columns (Actual Source)

1. SubmissionDate ✓
2. starttime
3. endtime
4. deviceid
5. devicephonenum
6. username
7. device_info
8. duration
9. caseid
10. forms_indonesia
11. location_indonesia
12. district_indonesia
13. observation_term
14. meeting
15. expected
16. malepresent
17. femalepresent
18. date
19. facilitator_role
20. facilitator_gender
21. facilitator_others
22. observer_role
23. observer_gender
24. observer_others
25. teacher_gender
26. teacher_others
27. s1, s2, s3, s4
28. e1, e2
29. c1, c1a, c2, c2a, c3
30. se1, se2, se3, se4, se5
31. remarks
32. meeting_coaching
33. date_coaching
34. duration_coaching
35. facilitator_role_coaching
36. name_of_the_coach
37. role_coaching
38. name_of_the_coachee
39. coach_gender
40. coach_gender_specify
41. coachee_gender
42. coachee_gender_specify
43. cc1, cc2, cc3, cc4, cc5
44. remarks_coaching
45. cro1, cro2, cro3, cro4, cro5
46. cro7, cro7a, cro8, cro8a, cro9
47. cro10, cro11, cro12
48. cro13a, cro13av, cro13b, cro13c
49. remarks_classroom
50. instanceID
51. formdef_version
52. formdef_id
53. review_quality
54. KEY
55. ge_1, ge_2, ge_3, ge_4, ge_5
56. programme
57. type_school
58. gc1, gc2, gc3, gc4, gc5
59. remarks_group_coaching

## Indonesia_Kobo Columns (Actual Source)

1. starttime
2. endtime
3. deviceid
4. devicephonenum
5. username
6. device_det
7. interview_dur
8. programme
9. forms_indonesia
10. location_indonesia
11. district_indonesia
12. type_school
13. observation_term
14. meeting
15. n_seca
16. expected
17. malepresent
18. femalepresent
19. date
20. facilitator_role
21. facilitator_gender
22. facilitator_others
23. observer_role
24. observer_gender
25. observer_others
26. date_cro
27. teacher_gender
28. teacher_others
29. n1, s1, s2, s3, s4, n2
30. e1, e2, n3
31. c1, c1a, c2, c2a, c3, n4
32. se1, se2, se3, se4, se5
33. ge_1, ge_2, ge_3, ge_4, ge_5
34. remarks
35. n_secc
36. meeting_coaching
37. date_coaching
38. duration_coaching
39. facilitator_role_coaching
40. name_of_the_coach
41. role_coaching
42. name_of_the_coachee
43. coach_gender
44. coach_gender_specify
45. coachee_gender
46. coachee_gender_specify
47. n1_secd
48. cc1, cc2, cc3, cc4, cc5
49. remarks_coaching
50. n1_sece
51. cro1, cro2, cro3
52. n2_sece
53. cro4, cro5
54. n3_sece
55. cro7, cro7a, cro8, cro8a, cro9
56. n4_sece
57. cro10, cro11, cro12
58. n5_sece
59. cro13a
60. cro13a/collab_&_coop_learning
61. cro13a/pairwork_&_groupwork
62. cro13a/settlers_&_stirrers
63. cro13a/differentiated_instruction
64. cro13a/digital_learning
65. cro13b
66. cro13c
67. remarks_classroom
68. n2_secf
69. gc1, gc2, gc3, gc4, gc5
70. remarks_group_coaching
71. start
72. end
73. phonenumber
74. device_info
75. duration
76. cro13av
77. cro13av/collab_&_coop_learning
78. cro13av/pairwork_&_groupwork
79. cro13av/settlers_&_stirrers
80. cro13av/differentiated_instruction
81. cro13av/digital_learning
82. _id
83. _uuid
84. _submission_time
85. _validation_status
86. _notes
87. _status
88. _submitted_by
89. __version__
90. _tags
91. _index

## Key Differences

### Columns ONLY in SurveyCTO:
- SubmissionDate ✓
- caseid
- instanceID
- formdef_version
- formdef_id
- review_quality
- KEY (but Kobo has _id)

### Columns ONLY in Kobo:
- device_det
- interview_dur
- n_seca, n_secc, n1, n2, n3, n4, n1_secd, n1_sece, n2_sece, n3_sece, n4_sece, n5_sece, n2_secf
- date_cro
- start, end
- phonenumber
- cro13a/collab_&_coop_learning (and other cro13a variants)
- cro13av/collab_&_coop_learning (and other cro13av variants)
- _id, _uuid, _submission_time, _validation_status, _notes, _status, _submitted_by, __version__, _tags, _index

### Columns in BOTH:
- starttime, endtime, deviceid, devicephonenum, username, device_info, duration
- forms_indonesia, location_indonesia, district_indonesia
- observation_term, meeting, expected
- malepresent, femalepresent, date
- facilitator_role, facilitator_gender, facilitator_others
- observer_role, observer_gender, observer_others
- teacher_gender, teacher_others
- s1-s4, e1-e2, c1-c3, se1-se5
- remarks, meeting_coaching, date_coaching, duration_coaching
- facilitator_role_coaching, name_of_the_coach, role_coaching, name_of_the_coachee
- coach_gender, coach_gender_specify, coachee_gender, coachee_gender_specify
- cc1-cc5, remarks_coaching
- cro1-cro12, cro13a, cro13av, cro13b, cro13c
- remarks_classroom
- ge_1-ge_5, programme, type_school
- gc1-gc5, remarks_group_coaching

## Issues to Address

1. **Missing _id in SurveyCTO**: SurveyCTO has KEY but not _id. Need to handle this.
2. **Kobo cro13a variants**: Have slashes in names (cro13a/collab_&_coop_learning) - need proper quoting
3. **Column order**: Need to ensure both CTEs have same column order for UNION ALL
