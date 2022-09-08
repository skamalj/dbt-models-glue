{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='hudi',
    unique_key='id',
    hudi_options={
       'hoodie.datasource.write.table.type':'MERGE_ON_READ',
       'hoodie.datasource.write.precombine.field': 'eventtime',
       'hoodie.combine.before.insert':'true'
   },
) }}

select *
from {{ source('flights','huditest') }}
{% if is_incremental() %}
    where eventtime > (select max(eventtime) from {{ this }})
{% endif %}
