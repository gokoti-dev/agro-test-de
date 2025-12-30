{{ config(
    materialized='table',
    schema='dds',
    post_hook="
       alter table {{ this }}
       add primary key (org_base_sk)
    "
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
			reg_number,
			org_name,
			org_short_name,
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
			reg_number,
			org_name,
			org_short_name,
			start_dt,
			end_dt,
			lag(reg_number, 1 ,reg_number) over (
				partition by vdd.org_sk
				order by start_dt
			) as
				lag_reg_number,
			lag(org_name, 1 ,org_name) over (
				partition by vdd.org_sk
				order by start_dt
			) as
				lag_org_name,
			lag(org_short_name, 1 ,org_short_name) over (
				partition by vdd.org_sk
				order by start_dt
			) as
				lag_org_short_name,
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
			reg_number,
			org_name,
			org_short_name,
			start_dt,
			end_dt,
			case
				when
					reg_number = lag_reg_number and
					org_name = lag_org_name and
					org_short_name = lag_org_short_name and
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
			reg_number,
			org_name,
			org_short_name,
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
			'11111111-1111-1111-1111-111111111111',
			org_sk || min(start_dt)::text
		) as
			org_base_sk,
		org_sk,
		min(reg_number) as reg_number,
		min(org_name) as org_name,
		min(org_short_name) as org_short_name,
		min(start_dt) as start_dt,
		max(end_dt) as end_dt,
		now()::timestamptz as load_ts
from
		grouped_data
group by
		org_sk,
		group_id
