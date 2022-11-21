{{ config(materialized='table') }}
{%- set yaml_metadata -%}
source_model: 'chdrpf_lgc_tbl'
derived_columns:
    RECORD_SOURCE: '!CHDRPF'
    EFFECTIVE_FROM: CURRENT_TIMESTAMP()
    LOAD_DATE: CURRENT_TIMESTAMP()
hashed_columns:
    hk_chdr_cd: 'chdrnum'
    rcrd_hsh_id:
        - chdrcoy
        - chdrnum
        - recode
        - servunit
        - cnttype
        - tranno
        - validflag
        - currfrom
        - currto
        - proctrancd
        - procflag
        - procid
        - statcode
        - statreasn
        - statdate
        - stattran
        - tranlused
        - occdate
        
{%- endset -%}
{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set source_model = metadata_dict['source_model'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
              hashed_columns=hashed_columns,
                  ranked_columns=none) }}
