
{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]
) }}

{{ dbt_utils.deduplicate(
    relation=ref('delhi_india'),
    partition_by='_airbyte_ab_id',
    order_by='_airbyte_emitted_at desc',
   )
}}