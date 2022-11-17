{{ config(
    materialized='table'
) }}

select *
from {{ ref('hudi_from_hudi') }}