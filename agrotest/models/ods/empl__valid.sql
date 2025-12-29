{{ config(
    materialized='table',
    schema='ods',
    pre_hook="{{ empl__snps__recreate_partition_and_insert() }}"
) }}

select
  tin::text,
  year::int4,
  employees_count::int4,
  '{{ var("input_data_date") }}'::date as input_data_date,
  now()::timestamptz as load_ts
from {{ source('stg', 'empl') }}
