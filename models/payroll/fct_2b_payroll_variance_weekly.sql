with current_payrun as (
    select * from {{ ref('int_payruns_weekly_current') }}
),

current_ts as (
    select * from {{ ref('payroll_export') }}
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

variance as (
    SELECT
        p.weekending,
        p.employee_id,
        p.employee,
        p.paid_hours as py_hrs,
        d.total_hours as d_hrs,
        p.gross_earnings as py_gross,
        round(d.total_cost, 2) as d_cost,
        (p.paid_hours - d.total_hours) as diff_in_hours,
        (p.gross_earnings - d.total_cost) as diff_in_cost
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
