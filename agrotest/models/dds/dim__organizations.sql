{{ config(
    materialized='table',
    schema='dds',
    post_hook="
       alter table {{ this }}
       add primary key (org_sk)
    "
) }}

with tins_data as (
	select
			distinct tin
	from
			{{ ref('smb__valid') }}
	union
	select
			distinct tin
	from
			{{ ref('empl__valid') }}
	union
	select
			distinct tin
	from
            {{ ref('revexp__valid') }}
)
select
		uuid_generate_v5(
			'00000000-0000-0000-0000-000000000000',
			tin
		) as
			org_sk,
		tin,
		now()::timestamptz as load_ts
from
		tins_data
