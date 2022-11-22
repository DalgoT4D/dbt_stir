{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]

) }}


{{ dbt_utils.union_relations(
    relations=[ref('int_classroom_surveys_uganda'),ref('int_classroom_surveys_indonesia'), ref('int_classroom_surveys_delhi'), ref('int_classroom_surveys_tamil_nadu'), ref('int_classroom_surveys_karnataka')],
    exclude=["_airbyte_emitted_at", "_airbyte_normalized_at"]
) }}