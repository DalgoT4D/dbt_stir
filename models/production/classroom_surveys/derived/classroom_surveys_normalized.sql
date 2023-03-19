{{ config(
  materialized='table'
) }}


with merged_normalized AS
       (SELECT "KEY","submissiondate","country","region","sub_region","forms","observation_term","meeting",
       CASE 
        When forms = 'nm_indo' then 'network_meeting_indonesia'
        When forms = 'cc_indo' then 'coaching_calls_indonesia'
        When forms = 'cro_indo' then 'classroom_observation_indonesia'
        When forms = 'dcm_indo' then 'district_coordination_meeting_indonesia'
        When forms = 'cc' then 'coaching_calls'
        When forms = 'cro' then 'classroom_observations'    
        When forms = 'nm' then 'network_meeting'
        When forms = 'nm_coart' then 'network_meeting_co-academic'
        When forms = 'dmpc' then 'district_monthly_progress_checks'
        When forms = 'nm' then 'network_meeting'
        When forms = 'nm_art' then 'network_meeting_academic'
        When forms = 'el_ins' then 'education_leaders_institute'
        When forms = 'elm_ins' then 'education_leader_manager_institute'
        When forms = 'dam' then 'district_alignment_meeting'
        When forms = 'dc_ins' then 'district_champion_institute'
        When forms = 'cc_ug' then 'coaching_calls_uganda'
        When forms = 'del_ins' then 'district_education_leader_institute'
        When forms = 'cct_ins' then 'center_coordinating_tutors_institute'
        When forms = 'sel_ins' then 'school_education_leader_institute'
        When forms = 'asshu_nb' then 'association_of_secondary_school_headteachers_of_uganda_national_bootcamp'
        When forms = 'dam_ug' then 'district_alignment_meeting_uganda'
        When forms = 'nm_ug' then 'network_meeting_uganda'
        When forms = 'cro_ug' then 'classroom_observation_uganda'
        When forms = 'mid_term_ug' then 'mid_term_meetups_Uganda'
       END As forms_verbose,
       unnest(array['s1','s2','s3','s4',
                    'c1','c2','c3',
                    'e1','e2',
                    'se1','se2','se3','se4','se5',
                    'cc1','cc2', 'cc3', 'cc4', 'cc5',
                    'ad1', 'ad2', 'ad3', 'ad4', 'ad5','ad7', 'ad8', 'ad9',
                    'sr1', 'sr2', 'sr3', 'sr4', 'sr5', 'sr6',
                    'cro13ai_behavior_engagement','cro13ai_behavior_safety','cro13ai_behavior_selfesteem',
                    'cro13ai_building_a_stronger_community','cro13ai_building_connect','cro13ai_classroom_routines',
                    'cro13ai_lesson_planning','cro13ai_look_for_understanding___respond','cro13ai_mission_buniyad',
                    'cro13ai_psychological_safety','cro13ai_social___emotional_wellbeing','cro13ai_teaching___learning_strategies_1',
                    'cro13ai_teaching___learning_strategies_2',
                    'cro13aiii_asking_effective_questions','cro13aiii_elaborative_questioning','cro13aiii_emotional_learning_environment',
                    'cro13aiii_growth_mindset','cro13aiii_physical_learning_environment','cro13aiii_retrieval_practices',
                    'cro13aiii_worked_examples',
                    'cro13aiv_bridging_covid19_learning_losses','cro13aiv_classroom_routines','cro13aiv_longterm_learning',
                    'cro13aiv_na','cro13aiv_socio_emotional_wellbeing','cro13aiv_structuring_learning_journey',
                    'cro13av_growth_mindset','cro13av_normalising_error',
                    'cro13b','cro13c']) AS subindicator,
       unnest(array[s1,s2,s3,s4,
                    c1,c2,
                    e1,e2,
                    se1,se2,se3,se4,se5,
                    cc1,cc2,cc3, cc4,cc5,
                    ad1, ad2, ad3, ad4, ad5,ad7, ad8, ad9,
                    sr1, sr2, sr3, sr4, sr5, sr6,
                    cro13ai_behavior_engagement,cro13ai_behavior_safety,cro13ai_behavior_selfesteem,
                    cro13ai_building_a_stronger_community,cro13ai_building_connect,cro13ai_classroom_routines,
                    cro13ai_lesson_planning,cro13ai_look_for_understanding___respond,cro13ai_mission_buniyad,
                    cro13ai_psychological_safety,cro13ai_social___emotional_wellbeing,cro13ai_teaching___learning_strategies_1,
                    cro13ai_teaching___learning_strategies_2,
                    cro13aiii_asking_effective_questions,cro13aiii_elaborative_questioning,cro13aiii_emotional_learning_environment,
                    cro13aiii_growth_mindset,cro13aiii_physical_learning_environment,cro13aiii_retrieval_practices,
                    cro13aiii_worked_examples,
                    cro13aiv_bridging_covid19_learning_losses,cro13aiv_classroom_routines,cro13aiv_longterm_learning,
                    cro13aiv_na,cro13aiv_socio_emotional_wellbeing,cro13aiv_structuring_learning_journey,
                    cro13av_growth_mindset,cro13av_normalising_error,
                    cro13b,cro13c]) AS score
FROM {{ref('classroom_surveys_merged')}}
)

select *, 
CASE 
WHEN subindicator IN ('s1','s2','s3','s4') THEN 'Safety' 
WHEN subindicator IN ('c1','c2','c3') THEN 'Curiosity & Critical Thinking'
WHEN subindicator IN ('e1','e2') THEN 'Engagement'
WHEN subindicator IN ('se1','se2', 'se3', 'se4', 'se5') THEN 'Self Esteem'
WHEN subindicator IN ('cc1','cc2', 'cc3', 'cc4', 'cc5') THEN 'Intentional Teaching'
WHEN subindicator IN ('ad1', 'ad2', 'ad3', 'ad4', 'ad5','ad7', 'ad8', 'ad9') THEN 'Delhi Additional Indicators'
WHEN subindicator IN ('sr1', 'sr2', 'sr3', 'sr4', 'sr5', 'sr6') THEN 'Delhi Co-ART Meeting Indicators'
WHEN subindicator IN ('cro13ai_behavior_engagement','cro13ai_behavior_safety','cro13ai_behavior_selfesteem',
                    'cro13ai_building_a_stronger_community','cro13ai_building_connect','cro13ai_classroom_routines',
                    'cro13ai_lesson_planning','cro13ai_look_for_understanding___respond','cro13ai_mission_buniyad',
                    'cro13ai_psychological_safety','cro13ai_social___emotional_wellbeing','cro13ai_teaching___learning_strategies_1',
                    'cro13ai_teaching___learning_strategies_2',
                    'cro13aiii_asking_effective_questions','cro13aiii_elaborative_questioning','cro13aiii_emotional_learning_environment',
                    'cro13aiii_growth_mindset','cro13aiii_physical_learning_environment','cro13aiii_retrieval_practices',
                    'cro13aiii_worked_examples',
                    'cro13aiv_bridging_covid19_learning_losses','cro13aiv_classroom_routines','cro13aiv_longterm_learning',
                    'cro13aiv_na','cro13aiv_socio_emotional_wellbeing','cro13aiv_structuring_learning_journey',
                    'cro13av_growth_mindset','cro13av_normalising_error','cro13b','cro13c') THEN 'Additional CRO Indicators'
ELSE 'Other' END AS behavior from merged_normalized
ORDER BY "submissiondate","KEY"


