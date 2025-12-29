{% macro revexp__snps__recreate_partition_and_insert() %}
  {% if not execute %}
    {{ return('') }}
  {% endif %}
  {% set date_str = var('input_data_date') %}
  {% set dt = modules.datetime.datetime.strptime(date_str, '%Y-%m-%d').date() %}
  {% set dt_next = dt + modules.datetime.timedelta(days=1) %}

  {% set parent = 'ods__revexp__valid' %}
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
      year,
      revenue,
      expenditure,
      input_data_date,
      load_ts
    )
    select
      tin::text,
      year::int4,
      revenue::float8,
      expenditure::float8,
      '{{ var("input_data_date") }}'::date as input_data_date,
      now()::timestamptz as load_ts
    from {{ source('stg', 'revexp') }};
  {% endset %}

  {% do run_query(sql) %}
{% endmacro %}
