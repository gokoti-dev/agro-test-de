{% macro smb__snps__recreate_partition_and_insert() %}
  {% if not execute %}
    {{ return('') }}
  {% endif %}
  {% set date_str = var('input_data_date') %}
  {% set dt = modules.datetime.datetime.strptime(date_str, '%Y-%m-%d').date() %}
  {% set dt_next = dt + modules.datetime.timedelta(days=1) %}

  {% set parent = 'ods__smb__valid' %}
  {% set part_name = parent ~ '__p' ~ dt.strftime('%Y%m%d') %}

  {% set sql %}
    -- 1) Recreate partition
    drop table if exists snps.{{ part_name }};

    create table snps.{{ part_name }}
      partition of snps.{{ parent }}
      for values from ('{{ dt.strftime("%Y-%m-%d") }}') to ('{{ dt_next.strftime("%Y-%m-%d") }}');

    -- 2) Insert snapshot rows into snps parent (will route to partition)
    insert into snps.{{ parent }} (
      tin,
      reg_number,
      category,
      org_name,
      org_short_name,
      activity_code_main,
      region,
      area,
      settlement,
      settlement_type,
      oktmo,
      lat,
      lon,
      address_raw,
      start_date,
      end_date,
      input_data_date,
      load_ts
    )
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
    from {{ source('stg', 'smb') }};
  {% endset %}

  {% do run_query(sql) %}
{% endmacro %}
