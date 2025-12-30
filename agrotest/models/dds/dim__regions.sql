{{ config(
    materialized='table',
    schema='dds',
    post_hook="
       alter table {{ this }}
       add primary key (geo_sk)
    "
) }}

with uniq_geo_data as (
	select
			distinct region,
			area,
			settlement,
			settlement_type,
			oktmo,
			lat,
			lon
	from
			{{ ref('smb__valid') }}
)
select
		uuid_generate_v5(
			'99999999-9999-9999-9999-999999999999',
			region ||
			coalesce(area, '__null__') ||
			coalesce(settlement, '__null__') ||
			coalesce(settlement_type, '__null__') ||
			coalesce(oktmo, '__null__') ||
			coalesce(lat::text, '__null__') ||
			coalesce(lon::text, '__null__')
		) as
			geo_sk,
		region,
		area,
		settlement,
		settlement_type,
		oktmo,
		lat,
		lon,
		now()::timestamptz as load_ts
from
		uniq_geo_data
