with current_payrun as (
    select * from {{ ref('int_payruns_weekly_current') }}
),

last_payrun as (
    select *
	from {{ source('public', 'payruns_weekly_ft') }}
	where weekending = (select max(weekending) - interval '7 days' 
                        from {{ source('public', 'payruns_weekly_ft') }})
),

current_ts as (
    select * from {{ ref('payroll_export') }}
),

current_ppe as (
	select
	 	weekending,
	    employee_id,
	    employee,
	    SUM(
	        CASE
	            when pay_category ilike any(array['%allowance%', '%%workcover%', '%government%', '%loading%']) then 0
	            else units
	        END
	    ) as thisPay_hrs,
	    SUM(gross) as thisPay_gross
	from current_payrun
	group by 1, 2, 3
),

last_ppe as (
	select
	 	weekending,
	    employee_id,
	    employee,
	    SUM(
	        CASE
	            when pay_category ilike any(array['%allowance%', '%%workcover%', '%government%', '%loading%']) then 0
	            else units
	        END
	    ) as lastPay_hrs,
	    SUM(gross) as lastPay_gross
	from last_payrun
	group by 1, 2, 3
),

payroll_report as (
    select
        weekending,
        employee_id,
        employee,
        SUM(
            CASE
                when pay_category ilike '%allowance%' then 0
                else units
            END
        ) as paid_hours,
        SUM(gross) as gross_earnings
    from current_payrun
    where pay_category not ilike '%leave%'
        and pay_category not ilike '%not worked%'
        and pay_category not ilike '%workcover%'
    group by 1, 2, 3
),

deputy_report as (
    SELECT
        date_trunc('week', ts_date) + interval '6 days' as weekending,
        employee_id,
        staff as employee,
        sum(ts_total_hours) as total_hours,
        sum(ts_cost) as total_cost
    from current_ts
    GROUP BY 1, 2, 3
),

variance_between_pay_periods as (
	select
		c.weekending,
		coalesce(c.employee_id, l.employee_id) as employee_id,
	    coalesce(c.employee, l.employee) as employee,
	    l.lastPay_hrs
	    , c.thisPay_hrs
	    , (coalesce(c.thisPay_hrs, 0) - coalesce(l.lastPay_hrs, 0)) as diff_hrs
	    , l.lastPay_gross
	    , c.thisPay_gross
	    , (coalesce(c.thisPay_gross, 0) - coalesce(l.lastPay_gross, 0)) as diff_gross
    from current_ppe c
    full outer join last_ppe l
    on c.employee_id = l.employee_id
),

variance_between_deputy_and_eh as (
    SELECT
        p.weekending,
        p.employee_id,
        p.employee,
        p.paid_hours as py_worked_hrs,
        d.total_hours as d_hrs,
        p.gross_earnings as py_worked_gross,
        d.total_cost as d_cost,
        (p.paid_hours - d.total_hours) as diff_in_hrs,
        (p.gross_earnings - d.total_cost) as diff_in_cost
    FROM payroll_report p
    LEFT JOIN deputy_report d
        USING (weekending, employee_id, employee)
),

final as (
    select
        v1.weekending, v1.employee_id, v1.employee,
        v1.lastPay_hrs, v1.thisPay_hrs, v1.diff_hrs,
        v1.lastPay_gross, v1.thisPay_gross, v1.diff_gross,
        v2.py_worked_hrs, v2.d_hrs,
        v2.py_worked_gross, v2.d_cost,
        v2.diff_in_hrs, v2.diff_in_cost
    from variance_between_pay_periods v1
    left join variance_between_deputy_and_eh v2
        using (employee_id)
    order by v1.employee
)

select * from final
