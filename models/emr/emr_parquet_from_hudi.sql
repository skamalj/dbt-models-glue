{{ config(
    materialized='table'
) }}

select *
from {{ ref('emr_hudi_emp_mor') }}