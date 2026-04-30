with current_payrun as (
    select * from {{ ref('int_payruns_casual_current') }}
),

current_ts as (
    select * from {{ ref('payroll_export') }}
),

payroll_report as (
    select
        weekending,
        employee_id,
        employee,
        SUM(case
                when pay_category ilike '%allowance%' then 0
                when pay_category ilike '%workcover%' then 0
                else units END) paid_hours,
        SUM(gross) gross_earnings
    from current_payrun
    where pay_category not ilike '%leave%'
    group by 1, 2, 3
),

deputy_report as (
    SELECT
        date_trunc('week', ts_date) + interval '6 days' as weekending,
        employee_id,
        staff as employee,
        sum(ts_total_hours) total_hours,
        sum(ts_cost) total_cost
    FROM
        current_ts
    GROUP BY 1, 2, 3
),

variance as (
    SELECT
        p.weekending,
        p.employee_id,
        p.employee,
        p.paid_hours as py_hrs,
        d.total_hours as d_hrs,
        round(p.gross_earnings, 2) as py_gross,
        round(d.total_cost, 2) as d_cost,
        round(p.paid_hours - d.total_hours, 2) as diff_in_hours,
        round(p.gross_earnings - d.total_cost, 2) as diff_in_cost
    FROM payroll_report p
    LEFT JOIN deputy_report d
        USING (weekending, employee_id, employee)
    WHERE
        (p.paid_hours - d.total_hours) > 0.05
        or (p.paid_hours - d.total_hours) < -0.05
        or (p.gross_earnings - d.total_cost) > 1
        or (p.gross_earnings - d.total_cost) < -1
)

select * from variance
