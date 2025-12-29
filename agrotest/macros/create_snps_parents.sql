{% macro create_snps_parents() %}
  {% set sql %}

    create table if not exists snps.ods__smb__valid
    (
      tin text not null,
      reg_number text,
      category int4 not null,
      org_name text not null,
      org_short_name text,
      activity_code_main text not null,
      region text not null,
      area text,
      settlement text,
      settlement_type text,
      oktmo text,
      lat double precision,
      lon double precision,
      address_raw text not null,
      start_date date not null,
      end_date date not null,

      -- Partition key (snapshot date)
      input_data_date date not null,

      -- Audit
      load_ts timestamptz not null default now()
    )
    partition by range (input_data_date);

    create table if not exists snps.ods__revexp__valid
    (
      tin text not null,
      year int4 not null,
      revenue double precision not null,
      expenditure double precision not null,

      -- Partition key (snapshot date)
      input_data_date date not null,

      -- Audit
      load_ts timestamptz not null default now()
    )
    partition by range (input_data_date);

    create table if not exists snps.ods__empl__valid
    (
      tin text not null,
      year int4 not null,
      employees_count int4,

      -- Partition key (snapshot date)
      input_data_date date not null,

      -- Audit
      load_ts timestamptz not null default now()
    )
    partition by range (input_data_date);

  {% endset %}

  {% do run_query(sql) %}
{% endmacro %}
