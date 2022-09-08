{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='hudi',
    unique_key='id',
    options={
       'type': 'mor',
       'primaryKey': 'id'
   },
) }}


select *
from {{ source('flights','huditest') }}
{% if is_incremental() %}
    where eventtime > (select max(eventtime) from {{ this }})
{% endif %}
