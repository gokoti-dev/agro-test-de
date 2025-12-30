{{ config(
    materialized='table',
    schema='dm',
    post_hook="
       alter table {{ this }}
       add primary key (year, category, region, main_okved_class)
    "
) }}

select
		year,
		category,
		region,
		main_okved_class,
		count(*) as organizations_count,
		sum(revenue) as revenue_sum,
		sum(expenditure) as expenditure_sum,
		sum(employees_count) as employees_count_sum
from
		{{ ref('org_year_profile') }}
group by
		year,
		category,
		region,
		main_okved_class
