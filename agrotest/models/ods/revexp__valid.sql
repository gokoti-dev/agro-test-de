{{ config(
    materialized='table',
    schema='ods',
    pre_hook="{{ revexp__snps__recreate_partition_and_insert() }}"
) }}

select
  tin::text,
  year::int4,
  revenue::float8,
  expenditure::float8,
  '{{ var("input_data_date") }}'::date as input_data_date,
  now()::timestamptz as load_ts
from {{ source('stg', 'revexp') }}
