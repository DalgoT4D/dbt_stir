{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]

) }}

with initial_merge as ({{ dbt_utils.union_relations(
    relations=[ref('int_classroom_surveys_uganda'),ref('int_classroom_surveys_indonesia'), ref('int_classroom_surveys_delhi'), ref('int_classroom_surveys_tamil_nadu'), ref('int_classroom_surveys_karnataka')],
    exclude=["_airbyte_emitted_at", "_airbyte_normalized_at"]
) }})


select *, 
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
       END As forms_verbose

from initial_merge 
      