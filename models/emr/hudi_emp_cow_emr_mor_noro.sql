{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='hudi',
    unique_key='id',
    partition_by='dept',
    options={
       'type': 'mor',
       'primaryKey': 'id',
       'preCombineField': 'eventtime',
       'hoodie.datasource.hive_sync.skip_ro_suffix' : 'true'
   },
) }}


select *
from {{ ref('emp_raw_inc_hudi') }}
{% if is_incremental() %}
    where eventtime > (select max(eventtime) from {{ this }})
{% endif %}
