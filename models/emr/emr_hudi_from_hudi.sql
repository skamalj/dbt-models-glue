{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='hudi',
    unique_key='id',
    partition_by='dept',
    merge_exclude_columns = ['_hoodie_commit_time'],
    options={
       'type': 'mor',
       'primaryKey': 'id',
       'preCombineField': 'eventtime',
       'hoodie.datasource.hive_sync.skip_ro_suffix' : 'true'
   },
) }}

select id,firstname,dept,lastname,phone,email,pincode,joiningdate,eventtime,from_unixtime(eventtime/1000) updated_at
from {{ ref('emr_hudi_emp_mor') }}
{% if is_incremental() %}
    where eventtime > (select max(eventtime) from {{ this }})
{% endif %}
