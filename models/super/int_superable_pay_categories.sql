with
leave_loading_pay_category as (
	select distinct pay_category_id
	from {{ ref('quarterly_pay_categories') }}
	where pay_category = 'Leave Loading'
),
superable_pay_categories as (
	select id
	from {{ ref('dim_pay_categories') }}
	where default_super_rate <> 0
	union all
	select *
	from leave_loading_pay_category 
)
select
	date_paid,
	employee_id,
	first_name || ' ' || surname as employee,
	sum(units) filter (where pay_category not ilike '%allowance%') as total_hours,
	sum(amount)::numeric as superable_gross,
	sum(sg_super)::numeric as sg_super
from {{ ref('quarterly_pay_categories') }}
where pay_category_id in (select * from superable_pay_categories)
group by 1, 2, 3