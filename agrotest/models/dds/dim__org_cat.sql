{{ config(
    materialized='table',
    schema='dds'
) }}

with marked_doubles_data as (
	select
			*,
			row_number() over(
				partition by tin, reg_number, category, org_name,
					org_short_name, activity_code_main, region,
					area, settlement, settlement_type, oktmo,
					lat, lon, start_date, end_date
				order by start_date
			) as
				dedup_id
	from
			{{ ref('smb__valid') }}
), valid_dates_data as (
	select
			uuid_generate_v5(
				'00000000-0000-0000-0000-000000000000',
				tin
			) as
				org_sk,
			category,
			date_trunc('month', start_date)::date as start_dt,
			(date_trunc('month', end_date) + interval '1 month')::date as end_dt,
			row_number() over(
				partition by tin
				order by start_date
			) as
				org_row_id
	from
			marked_doubles_data
	where
			date_trunc('month', start_date)::date <
				(date_trunc('month', end_date) + interval '1 month')::date
		and
			dedup_id = 1
), not_valid_data as (
	select
			distinct x.org_sk, x.org_row_id
	from
			valid_dates_data as x
	inner join
			valid_dates_data as y
		on
				x.org_sk = y.org_sk
			and
				x.org_row_id <> y.org_row_id
	where
			x.start_dt < y.end_dt
		and
			y.start_dt < x.end_dt
), full_valid_data as (
	select
			vdd.org_sk,
			category,
			start_dt,
			end_dt,
			lag(category, 1 ,category) over (
				partition by vdd.org_sk
				order by start_dt
			) as
				lag_category,
			lag(end_dt, 1 ,start_dt) over (
				partition by vdd.org_sk
				order by start_dt
			) as
				lag_end_dt
	from
			valid_dates_data as vdd
	left join
			not_valid_data as nvd
		on
			vdd.org_sk = nvd.org_sk and vdd.org_row_id = nvd.org_row_id
	where
			nvd.org_sk is null
), equal_lags_marked_data as (
	select
			org_sk,
			category,
			start_dt,
			end_dt,
			case
				when
					category = lag_category and
					start_dt = lag_end_dt
				then
					0
				else
					1
			end as
				as_lag
	from
			full_valid_data
), grouped_data as (
	select
			org_sk,
			category,
			start_dt,
			end_dt,
			sum(as_lag) over(
				partition by org_sk
				order by start_dt
			) as
				group_id
	from
			equal_lags_marked_data
)
select
		uuid_generate_v5(
			'33333333-3333-3333-3333-333333333333',
			org_sk || min(start_dt)::text
		) as
			org_cat_sk,
		org_sk,
		min(category) as category,
		min(start_dt) as start_dt,
		max(end_dt) as end_dt,
		now()::timestamptz as load_ts
from
		grouped_data
group by
		org_sk,
		group_id
