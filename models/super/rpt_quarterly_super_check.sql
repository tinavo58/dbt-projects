with
super_report as (
	select
		employee_id,
		employee_name,
		sum(amount) filter (where contribution_type = 'Super Guarantee') as super_guarantee,
		sum(amount) filter (where contribution_type = 'Salary Sacrifice') as salary_sacrifice,
		sum(amount) as super_reported
	from {{ ref('quarterly_super') }}
	group by 1, 2
),
pay_categories_report as (
	select
		employee_id,
		employee,
		sum(superable_gross) as superable_gross,
		case
			when sum(superable_gross) * 12 /100 > 7500 then 7500
			else round(sum(superable_gross) * 12 /100, 2)
		end as super_check
	from {{ ref('int_superable_pay_categories') }}
	group by 1, 2
)
select
	pcr.employee_id,
	pcr.employee,
	pcr.superable_gross,
	pcr.super_check,
	sr.super_guarantee,
	coalesce(sr.super_guarantee, 0) - pcr.super_check as super_diff
from pay_categories_report pcr
left join super_report sr
	using(employee_id)
where coalesce(sr.super_guarantee, 0) - pcr.super_check > 0.05
	or coalesce(sr.super_guarantee, 0) - pcr.super_check < -0.05
order by coalesce(sr.super_guarantee, 0) - pcr.super_check desc