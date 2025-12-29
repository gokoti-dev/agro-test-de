{{ config(
    materialized='table',
    schema='ods',
    pre_hook="{{ smb__snps__recreate_partition_and_insert() }}"
) }}

select
  tin::text,
  reg_number::text,
  category::int4,
  org_name::text,
  org_short_name::text,
  activity_code_main::text,
  region::text,
  area::text,
  settlement::text,
  settlement_type::text,
  oktmo::text,
  lat::float8,
  lon::float8,
  address_raw::text,
  start_date::date,
  end_date::date,
  '{{ var("input_data_date") }}'::date as input_data_date,
  now()::timestamptz as load_ts
from {{ source('stg', 'smb') }}
