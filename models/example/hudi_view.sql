{{ config(
    materialized='view'
) }}


select *
from {{ source('flights','huditest') }}
{% if is_incremental() %}
    where eventtime > (select max(eventtime) from {{ this }})
{% endif %}
