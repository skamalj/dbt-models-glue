{{ config(
    materialized='incremental'
) }}

select *
from {{ source('flights','emp_raw') }}
