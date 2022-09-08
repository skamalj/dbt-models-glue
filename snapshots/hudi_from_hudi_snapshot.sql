{% snapshot hudi_from_hudi_snapshot %}

{{
    config(
      target_database='flights',
      target_schema='flights',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ ref('hudi_from_hudi') }}

{% endsnapshot %}