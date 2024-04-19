{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ]

) }}

with initial_merge as ({{ dbt_utils.union_relations(
    relations=[ref('int_classroom_surveys_uganda'),ref('int_classroom_surveys_indonesia'), ref('int_classroom_surveys_delhi'), ref('int_classroom_surveys_tamil_nadu'), ref('int_classroom_surveys_karnataka'), ref('int_classroom_surveys_ethiopia')],
    exclude=["_airbyte_emitted_at", "_airbyte_normalized_at"]
) }})


select *, 
     CASE 
        When forms = 'nm_indo' then 'Network Meeting Indonesia'
        When forms = 'cc_indo' then 'Coaching Calls Indonesia'
        When forms = 'cro_indo' then 'Classroom Observation Indonesia'
        When forms = 'dcm_indo' then 'District Coordination Meeting Indonesia'
        When forms = 'cc' then 'Coaching Calls'
        When forms = 'cro' then 'Classroom Observations'    
        When forms = 'nm' then 'Network Meeting'
        When forms = 'nm_coart' then 'Network Meeting Co-Academic'
        When forms = 'dmpc' then 'District Monthly Progress Checks'
        When forms = 'nm' then 'Network Meeting'
        When forms = 'nm_art' then 'Network Meeting Academic'
        When forms = 'el_ins' then 'Education Leaders Institute'
        When forms = 'elm_ins' then 'Education Leader Manager Institute'
        When forms = 'dam' then 'District Alignment Meeting'
        When forms = 'dc_ins' then 'District Champion Institute'
        When forms = 'cc_ug' then 'Coaching calls uganda'
        When forms = 'del_ins' then 'District Education Leader Institute'
        When forms = 'cct_ins' then 'Center Coordinating Tutors Institute'
        When forms = 'sel_ins' then 'School Education Leader Institute'
        When forms = 'asshu_nb' then 'Association of Secondary School Headteachers of Uganda National Bootcamp'
        When forms = 'dam_ug' then 'District Alignment Meeting Uganda'
        When forms = 'nm_ug' then 'Network Meeting Uganda'
        When forms = 'cro_ug' then 'Classroom Observation Uganda'
        When forms = 'mid_term_ug' then 'Mid Term Meetups Uganda'
        When forms = 'sp_indo' then 'School Principals Institute Indonesia'
        When forms = 'dpo_nb' then 'District Personnel Officer National Bootcamp'
        When forms = 'ss_indo' then 'School Supervisors Institute Indonesia'
        When forms = 'dcac_indo' then 'District Coordinator/Area Coordinator Institute Indonesia'
       END As forms_verbose

from initial_merge 
      
