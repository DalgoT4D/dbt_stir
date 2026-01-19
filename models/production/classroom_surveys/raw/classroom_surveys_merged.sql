{{ config(
  materialized='table'
) }}

with initial_merge as ({{ dbt_utils.union_relations(
    relations=[ref('int_classroom_surveys_uganda'), ref('int_classroom_surveys_indonesia'), ref('int_classroom_surveys_delhi'), ref('int_classroom_surveys_tamil_nadu'), ref('int_classroom_surveys_karnataka'), ref('int_classroom_surveys_ethiopia')],
    exclude=["_airbyte_emitted_at", "_airbyte_normalized_at"]
) }})

select *, 
    CASE 
        WHEN forms = 'sam' THEN 'State Alignment Meeting'
        WHEN forms = 'gc_indo' THEN 'Group Coaching Indonesia'
        WHEN forms = 'nm_eth' THEN 'Network Meeting Ethiopia'
        WHEN forms = 'cro_eth' THEN 'Classroom Observation Ethiopia'
        WHEN forms = 'cc_eth' THEN 'Coaching Calls Ethiopia'
        WHEN forms = 'nm_indo' THEN 'Network Meeting Indonesia'
        WHEN forms = 'cc_indo' THEN 'Coaching Calls Indonesia'
        WHEN forms = 'cro_indo' THEN 'Classroom Observation Indonesia'
        WHEN forms = 'dcm_indo' THEN 'District Coordination Meeting Indonesia'
        WHEN forms = 'cc' THEN 'Coaching Calls'
        WHEN forms = 'cro' THEN 'Classroom Observations'    
        WHEN forms = 'nm' THEN 'Network Meeting'
        WHEN forms = 'nm_coart' THEN 'Network Meeting Co-Academic'
        WHEN forms = 'dmpc' THEN 'District Monthly Progress Checks'
        WHEN forms = 'nm_art' THEN 'Network Meeting Academic'
        WHEN forms = 'el_ins' THEN 'Education Leaders Institute'
        WHEN forms = 'elm_ins' THEN 'Education Leader Manager Institute'
        WHEN forms = 'dam' THEN 'District Alignment Meeting'
        WHEN forms = 'dc_ins' THEN 'District Champion Institute'
        WHEN forms = 'cc_ug' THEN 'Coaching Calls Uganda'
        WHEN forms = 'del_ins' THEN 'District Education Leader Institute'
        WHEN forms = 'cct_ins' THEN 'Center Coordinating Tutors Institute'
        WHEN forms = 'sel_ins' THEN 'School Education Leader Institute'
        WHEN forms = 'asshu_nb' THEN 'Association of Secondary School Headteachers of Uganda National Bootcamp'
        WHEN forms = 'dam_ug' THEN 'District Alignment Meeting Uganda'
        WHEN forms = 'nm_ug' THEN 'Network Meeting Uganda'
        WHEN forms = 'cro_ug' THEN 'Classroom Observation Uganda'
        WHEN forms = 'mid_term_ug' THEN 'Mid Term Meetups Uganda'
        WHEN forms = 'sp_indo' THEN 'School Principals Institute Indonesia'
        WHEN forms = 'dpo_nb' THEN 'District Personnel Officer National Bootcamp'
        WHEN forms = 'ss_indo' THEN 'School Supervisors Institute Indonesia'
        WHEN forms = 'dcac_indo' THEN 'District Coordinator/Area Coordinator Institute Indonesia'
    END AS forms_verbose,
    CASE 
        WHEN forms = 'nm_indo' THEN 'Network Meeting'
        WHEN forms = 'nm_eth' THEN 'Network Meeting'
        WHEN forms = 'cc_indo' THEN 'Coaching Calls'
        WHEN forms = 'cro_indo' THEN 'Classroom Observation'
        WHEN forms = 'dcm_indo' THEN 'District Coordination Meeting'
        WHEN forms = 'cc' THEN 'Coaching Calls'
        WHEN forms = 'cro' THEN 'Classroom Observations'    
        WHEN forms = 'nm' THEN 'Network Meeting'
        WHEN forms = 'nm_coart' THEN 'Network Meeting Co-Academic'
        WHEN forms = 'dmpc' THEN 'District Monthly Progress Checks'
        WHEN forms = 'nm_art' THEN 'Network Meeting Academic'
        WHEN forms = 'el_ins' THEN 'Education Leaders Institute'
        WHEN forms = 'elm_ins' THEN 'Education Leader Manager Institute'
        WHEN forms = 'dam' THEN 'District Alignment Meeting'
        WHEN forms = 'dc_ins' THEN 'District Champion Institute'
        WHEN forms = 'cc_ug' THEN 'Coaching Calls'
        WHEN forms = 'cc_eth' THEN 'Coaching Calls'
        WHEN forms = 'del_ins' THEN 'District Education Leader Institute'
        WHEN forms = 'cct_ins' THEN 'Center Coordinating Tutors Institute'
        WHEN forms = 'sel_ins' THEN 'School Education Leader Institute'
        WHEN forms = 'asshu_nb' THEN 'Association of Secondary School Headteachers of Uganda National Bootcamp'
        WHEN forms = 'dam_ug' THEN 'District Alignment Meeting'
        WHEN forms = 'nm_ug' THEN 'Network Meeting'
        WHEN forms = 'cro_ug' THEN 'Classroom Observation'
        WHEN forms = 'cro_eth' THEN 'Classroom Observation'
        WHEN forms = 'mid_term_ug' THEN 'Mid Term Meetups'
        WHEN forms = 'sp_indo' THEN 'School Principals Institute'
        WHEN forms = 'dpo_nb' THEN 'District Personnel Officer National Bootcamp'
        WHEN forms = 'ss_indo' THEN 'School Supervisors Institute'
        WHEN forms = 'dcac_indo' THEN 'District Coordinator/Area Coordinator Institute'
    END AS forms_verbose_consolidated

from initial_merge
