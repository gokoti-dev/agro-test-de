{{ config(
    materialized='table',
    schema='dds'
) }}

with marked_doubles_data as (
	select
			*,
			row_number() over(
				partition by tin, year, revenue, expenditure
				order by year
			) as
				dedup_id
	from
			{{ ref('revexp__valid') }}
), valid_data as (
	select
			tin,
			year,
			min(revenue) as revenue,
			min(expenditure) as expenditure
	from
			marked_doubles_data
	where
			dedup_id = 1
	group by
			tin,
			year
	having
			count(*) = 1
)
select
		uuid_generate_v5(
			'55555555-5555-5555-5555-555555555555',
			tin || year::text
		) as
			org_fin_sk,
		uuid_generate_v5(
			'00000000-0000-0000-0000-000000000000',
			tin
		) as
			org_sk,
		year,
		revenue,
		expenditure,
		now()::timestamptz as load_ts
from
		valid_data
