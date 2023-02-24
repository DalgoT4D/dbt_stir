
  
    

  create  table "postgres"."prod_intermediate"."int_classroom_surveys_uganda__dbt_tmp"
  as (
    

select
"_airbyte_unique_key",
  "e3",
  "n1",
  "n2",
  "n3",
  "n4",
  "s4",
  "KEY",
  "cc1",
  "cc2",
  "cc3",
  "cc4",
  "cc5",
  "se4",
  "se5",
  "cro1",
  "cro2",
  "cro3",
  "cro4",
  "cro5",
  "cro7",
  "cro8",
  "cro9",
  "cro10",
  "cro11",
  "cro12",
  "cro7a",
  "cro8a",
  "b_secb",
  "caseid",
  "cro13a",
  "cro13b",
  "cro13c",
  "n_seca",
  "n_secc",
  "plname",
  "meeting",
  "n1_secd",
  "n1_sece",
  "n2_sece",
  "n3_sece",
  "n4_sece",
  "n5_sece",
  "remarks",
  "deviceid",
  "duration",
  "expected",
  "username",
  "instanceid",
  "device_info",
  "malepresent",
  "coach_gender",
  "forms_uganda",
  "district_teso",
  "femalepresent",
  "observer_role",
  "review_status",
  "role_coaching",
  "coachee_gender",
  "devicephonenum",
  "district_lango",
  "district_mbale",
  "review_quality",
  "teacher_gender",
  "teacher_others",
  "district_acholi",
  "district_busoga",
  "district_kigezi",
  "district_masaka",
  "education_level",
  "formdef_version",
  "location_uganda",
  "observer_gender",
  "observer_others",
  "district_bunyoro",
  "district_central",
  "facilitator_role",
  "meeting_coaching",
  "observation_term",
  "remarks_coaching",
  "district_karamoja",
  "district_rwenzori",
  "district_westnile",
  "duration_coaching",
  "name_of_the_coach",
  "remarks_classroom",
  "facilitator_gender",
  "facilitator_others",
  "district_midwestern",
  "name_of_the_coachee",
  "cro13aiii_growth_mindset",
  "cro13aiii_spaced_practice",
  "cro13aiii_worked_examples",
  "facilitator_role_coaching",
  "cro13aiii_retrieval_practices",
  "cro13aiii_breaking_down_learning",
  "cro13aiii_elaborative_questioning",
  "cro13aiii_asking_effective_questions",
  "cro13aiii_giving___receiving_feedback",
  "cro13aiii_physical_learning_environment",
  "cro13aiii_emotional_learning_environment",
  "_airbyte_ab_id",
  "_airbyte_emitted_at",
  "_airbyte_normalized_at",
  "_airbyte_uganda_bm_2022_hashid",
'Uganda' AS country, location_uganda AS region, coalesce (district_bunyoro,district_kigezi,district_masaka, district_rwenzori, district_central) as sub_region,
COALESCE(s1, cro1) as s1,
COALESCE(s2, cro2) as s2,  COALESCE(s3, cro3) as s3, COALESCE(cro4, e1) as e1, COALESCE(cro5, e2) as e2, COALESCE(cro7, c1) as c1,
COALESCE(cro7a, c1a) as c1a, COALESCE(cro8, c2) as c2, COALESCE(cro8a, c2a) as c2a, COALESCE(cro10, se1) as se1,
COALESCE(cro11, se2) as se2, COALESCE(cro12, se3) as se3, to_date(coalesce(date,date_coaching), 'Mon, DD YYYY') as observation_date, forms_uganda as forms,
to_timestamp(starttime,'Mon, DD YYYY HH:MI:SS AM') AS starttime,
to_timestamp(endtime,'Mon, DD YYYY HH:MI:SS AM') AS endtime,
to_timestamp(completiondate,'Mon, DD YYYY HH:MI:SS AM') AS completiondate,
to_timestamp(submissiondate,'Mon, DD YYYY HH:MI:SS AM') AS submissiondate
from "postgres"."staging"."uganda_bm_2022"
  );
  