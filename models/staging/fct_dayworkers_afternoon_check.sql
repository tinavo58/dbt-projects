with staff as (
    select * from {{source('public', 'eh_staff')}}
),

timesheets as (
    select
        *
    from {{ ref('payroll_export') }}
),

non_shiftworkers as (
    select
        employee_id,
        first_name || ' ' || surname as staff_name
    from staff
    WHERE
        pay_condition_rule_set ilike '%non-shift%'
)

select
    we,
    location_name,
    staff,
    ts_date,
    ts_start,
    ts_end,
    ts_total_hours
from timesheets
where
    ts_start > '11:00'
    AND ts_end > '18:00'
    and employee_id in (select employee_id from non_shiftworkers)
