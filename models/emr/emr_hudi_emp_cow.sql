{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='hudi',
    unique_key='id',
    partition_by='dept',
    merge_exclude_columns = ['_hoodie_commit_time'],
    options={
       'type': 'cow',
       'primaryKey': 'id',
       'preCombineField': 'eventtime'
   },
) }}


select *
from {{ ref('emp_raw_inc_hudi') }}
{% if is_incremental() %}
    where eventtime > (select max(eventtime) from {{ this }})
{% endif %}
