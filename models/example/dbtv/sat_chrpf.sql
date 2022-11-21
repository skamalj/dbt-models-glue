{{ config(materialized='incremental',
file_format="hudi",
unique_key="hk_chdr_cd,LOAD_DATE",
hudi_options={'hoodie.datasource.write.table.type':'MERGE_ON_READ',
'hoodie.datasource.write.precombine.field': 'LOAD_DATE',
'hoodie.combine.before.insert':'true',
"hoodie.datasource.hive_sync.enable": "true",
'hoodie.datasource.write.partitionpath.field':'',
"hoodie.compact.inline": "true",
"hoodie.datasource.write.operation": "insert",
"hoodie.compact.inline.max.delta.commits": "1",
"hoodie.clean.async": "true",
"hoodie.clean.automatic": "true",
"hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
"hoodie.cleaner.commits.retained": "1",
"hoodie.datasource.write.keygenerator.class":'org.apache.hudi.keygen.ComplexKeyGenerator'})
}}
-- depends_on: {{ ref('chdrpf_lgc_stg') }}
{%- set source_model = 'chdrpf_lgc_stg' -%}
{%- set src_pk = "hk_chdr_cd" -%}
{%- set src_hashdiff = "rcrd_hsh_id" -%}
{%- set src_payload = [
"chdrcoy"
,"recode"
,"servunit"
,"cnttype"
,"tranno"
,"validflag"
,"currfrom"
,"currto"
,"proctrancd"
,"procflag"
,"procid"
,"statcode"
,"statreasn"
,"statdate"
,"stattran"
,"tranlused"
,"occdate"
] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
