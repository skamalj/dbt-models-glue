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

{# 
    Settings listed below can be added so hudi tables can be read from glue catalog.
    1. Add this to profile file sync_tool_classes: "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool"
    2. Add 'hive.metastore.schema.verification': 'false'  to model config
#}

select id,firstname,lastname,phone,email,pincode,joiningdate,eventtime,from_unixtime(eventtime/1000) updated_at
from {{ ref('hudi_emp_mor') }}
{% if is_incremental() %}
    where eventtime > (select max(eventtime) from {{ this }})
{% endif %}
