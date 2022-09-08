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

with 
source_data as (
    select rank() over(partition by id order by eventtime desc) as r, t.* from flights.huditest t
),
unique_source as (
    select id,firstname,lastname,joiningDate,phone,pincode,email,eventtime from source_data where r = 1
)

select *
from unique_source
{% if is_incremental() %}
    where eventtime > (select max(eventtime) from {{ this }})
{% endif %}
