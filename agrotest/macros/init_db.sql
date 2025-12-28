{% macro init_db() %}
  {% set sql %}
    create extension if not exists "uuid-ossp";
    create schema if not exists stg;
    create schema if not exists ods;
    create schema if not exists dds;
    create schema if not exists cdm;
    create schema if not exists dm;
  {% endset %}

  {% do run_query(sql) %}
{% endmacro %}
