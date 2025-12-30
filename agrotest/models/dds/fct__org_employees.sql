{{ config(
    materialized='table',
    schema='dds',
    post_hook="
       alter table {{ this }}
       add primary key (org_empl_sk)
    "
) }}

with marked_doubles_data as (
	select
			*,
			row_number() over(
				partition by tin, year, employees_count
				order by year
			) as
				dedup_id
	from
			{{ ref('empl__valid') }}
), valid_data as (
	select
			tin,
			year,
			min(employees_count) as employees_count
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
			'66666666-6666-6666-6666-666666666666',
			tin || year::text
		) as
			org_empl_sk,
		uuid_generate_v5(
			'00000000-0000-0000-0000-000000000000',
			tin
		) as
			org_sk,
		year,
		employees_count,
		now()::timestamptz as load_ts
from
		valid_data
