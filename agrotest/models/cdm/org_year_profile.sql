{{ config(
    materialized='table',
    schema='cdm',
    post_hook="
       alter table {{ this }}
       add primary key (org_sk, year)
    "
) }}

with orgs_min_max_years as (
	select
			org_sk,
			extract(year from min(start_dt)) as start_year,
			extract(year from max(end_dt)) as end_year
	from
			{{ ref('dim__org_base') }}
	group by
			org_sk
), pre_min_max_years as (
	select
			min(year) as min_year,
			max(year) as max_year
	from
			dds.fct__org_finance
	union all
	select
			min(year) as min_year,
			max(year) as max_year
	from
			dds.fct__org_employees
), min_max_years as (
	select
			min(min_year) as min_year,
			max(max_year) as max_year
	from
			pre_min_max_years
), years_seq as (
	select
			generate_series(min_year, max_year) as year
	from
			min_max_years
), orgs_years as (
	select
			oy.org_sk,
			gs.year as year
	from
			orgs_min_max_years as oy
	cross join lateral
			generate_series(oy.start_year, oy.end_year) as gs(year)
	inner join
			years_seq as ys
		on
			ys.year = gs.year
), org_cats_ext_1 as (
	select
			*,
			row_number() over(
				partition by org_sk
				order by start_dt
			) as
				is_first_period,
			row_number() over(
				partition by org_sk
				order by start_dt desc
			) as
				is_last_period
	from
			{{ ref('dim__org_cat') }}
), org_cats_ext_2 as (
	select
			org_sk,
			category,
			case
				when is_first_period = 1
					then date_trunc('year', start_dt)::date
				else
					start_dt
			end as
				start_dt,
			case
				when is_last_period = 1
					then (
						date_trunc('year', end_dt - 1) + interval '1 year'
					)::date
				else
					end_dt
			end as
				end_dt
	from
			org_cats_ext_1
), cats_data as (
	select
			t.org_sk,
			ys.year,
			t.category
	from
			org_cats_ext_2 as t
	inner join
			years_seq as ys
		on
				t.start_dt <= make_date(ys.year, 1, 1)
			and
				t.end_dt >= make_date(ys.year+1, 1, 1)
), regions_source_data as (
	select
			og.org_sk,
			region,
			start_dt,
			end_dt,
			lag(region, 1 ,region) over (
				partition by og.org_sk
				order by start_dt
			) as
				lag_region,
			lag(end_dt, 1 ,start_dt) over (
				partition by og.org_sk
				order by start_dt
			) as
				lag_end_dt
	from
			{{ ref('dim__org_geo') }} as og
	left join
			{{ ref('dim__regions') }} as rg
		on
			og.geo_sk = rg.geo_sk
), equal_lags_marked_data_regions as (
	select
			org_sk,
			region,
			start_dt,
			end_dt,
			case
				when
					region = lag_region and
					start_dt = lag_end_dt
				then
					0
				else
					1
			end as
				as_lag
	from
			regions_source_data
), grouped_data_regions as (
	select
			org_sk,
			region,
			start_dt,
			end_dt,
			sum(as_lag) over(
				partition by org_sk
				order by start_dt
			) as
				group_id
	from
			equal_lags_marked_data_regions
), regions_norm_data as (
	select
			org_sk,
			min(region) as region,
			min(start_dt) as start_dt,
			max(end_dt) as end_dt
	from
			grouped_data_regions
	group by
			org_sk,
			group_id
), org_regions_ext_1 as (
	select
			*,
			row_number() over(
				partition by org_sk
				order by start_dt
			) as
				is_first_period,
			row_number() over(
				partition by org_sk
				order by start_dt desc
			) as
				is_last_period
	from
			regions_norm_data
), org_regions_ext_2 as (
	select
			org_sk,
			region,
			case
				when is_first_period = 1
					then date_trunc('year', start_dt)::date
				else
					start_dt
			end as
				start_dt,
			case
				when is_last_period = 1
					then (
						date_trunc('year', end_dt - 1) + interval '1 year'
					)::date
				else
					end_dt
			end as
				end_dt
	from
			org_regions_ext_1
), regions_data as (
	select
			t.org_sk,
			ys.year,
			t.region
	from
			org_regions_ext_2 as t
	inner join
			years_seq as ys
		on
				t.start_dt <= make_date(ys.year, 1, 1)
			and
				t.end_dt >= make_date(ys.year+1, 1, 1)
), org_activity_ext_data as (
	select
			org_sk,
			split_part(activity_code_main, '.', 1) as main_okved_class,
			start_dt,
			end_dt
	from
			{{ ref('dim__org_activity') }}
), okved_source_data as (
	select
			org_sk,
			main_okved_class,
			start_dt,
			end_dt,
			lag(main_okved_class, 1 ,main_okved_class) over (
				partition by org_sk
				order by start_dt
			) as
				lag_main_okved_class,
			lag(end_dt, 1 ,start_dt) over (
				partition by org_sk
				order by start_dt
			) as
				lag_end_dt
	from
			org_activity_ext_data
), equal_lags_marked_data_okved as (
	select
			org_sk,
			main_okved_class,
			start_dt,
			end_dt,
			case
				when
					main_okved_class = lag_main_okved_class and
					start_dt = lag_end_dt
				then
					0
				else
					1
			end as
				as_lag
	from
			okved_source_data
), grouped_data_okved as (
	select
			org_sk,
			main_okved_class,
			start_dt,
			end_dt,
			sum(as_lag) over(
				partition by org_sk
				order by start_dt
			) as
				group_id
	from
			equal_lags_marked_data_okved
), okved_norm_data as (
	select
			org_sk,
			min(main_okved_class) as main_okved_class,
			min(start_dt) as start_dt,
			max(end_dt) as end_dt
	from
			grouped_data_okved
	group by
			org_sk,
			group_id
), org_okved_ext_1 as (
	select
			*,
			row_number() over(
				partition by org_sk
				order by start_dt
			) as
				is_first_period,
			row_number() over(
				partition by org_sk
				order by start_dt desc
			) as
				is_last_period
	from
			okved_norm_data
), org_okved_ext_2 as (
	select
			org_sk,
			main_okved_class,
			case
				when is_first_period = 1
					then date_trunc('year', start_dt)::date
				else
					start_dt
			end as
				start_dt,
			case
				when is_last_period = 1
					then (
						date_trunc('year', end_dt - 1) + interval '1 year'
					)::date
				else
					end_dt
			end as
				end_dt
	from
			org_okved_ext_1
), okved_data as (
	select
			t.org_sk,
			ys.year,
			t.main_okved_class
	from
			org_okved_ext_2 as t
	inner join
			years_seq as ys
		on
				t.start_dt <= make_date(ys.year, 1, 1)
			and
				t.end_dt >= make_date(ys.year+1, 1, 1)
)
select
		oy.org_sk,
		oy.year,
		coalesce(cd.category, -1) as category,
		coalesce(rd.region, 'UNKOWN') as region,
		coalesce(od.main_okved_class, 'UNKOWN') as main_okved_class,
		f.revenue,
		f.expenditure,
		emp.employees_count
from
		orgs_years as oy
left join
		cats_data as cd
	on
		(oy.org_sk, oy.year) =
			(cd.org_sk, cd.year)
left join
		regions_data as rd
	on
		(oy.org_sk, oy.year) =
			(rd.org_sk, rd.year)
left join
		okved_data as od
	on
		(oy.org_sk, oy.year) =
			(od.org_sk, od.year)
left join
		{{ ref('fct__org_finance') }} as f
	on
		(oy.org_sk, oy.year) =
			(f.org_sk, f.year)
left join
		{{ ref('fct__org_employees') }} as emp
	on
		(oy.org_sk, oy.year) =
			(emp.org_sk, emp.year)
