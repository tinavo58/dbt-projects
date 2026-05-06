with staff_base as (
    select * from {{ source('public', 'eh_staff') }}
),

selected_staff_details as (
    select
        first_name || ' ' || surname as staff,
        hours_per_week
    from staff_base
    where pay_schedule ilike '%weekly%'
),

work_patterns as (
    select * from {{ ref('dim_staff_work_patterns') }}
),

latest_we as (
    select max(week_ending) as we from {{ source('public', 'exception_report') }}
),

daily_worked as (
    select
        staff,
        ts_date,
        sum(ts_total_hours) as worked_hours
    from {{ ref('payroll_export') }}
    group by 1, 2
),

leave_on_day as (
    select
        staff,
        start_date as leave_date,
        duration as leave_hours,
        leave_type
    from {{ ref('leave_requests') }}
    -- TODO: create leave_req table
    where start_date >= (select we from latest_we) - 6
      and start_date <= (select we from latest_we)
),

combined as (
    select
        dw.staff,
        dw.ts_date,
        dw.worked_hours,
        l.leave_hours as entered_by_staff,
        l.leave_type,
        dw.worked_hours + l.leave_hours as total_hours,
        case
            when s.hours_per_week >= 38 then 7.6
            else wp.daily_hours
        end as expected_hours
    from daily_worked as dw
    inner join leave_on_day as l
        on dw.staff = l.staff
        and dw.ts_date = l.leave_date
    inner join selected_staff_details as s
        on dw.staff = s.staff
    left join work_patterns as wp
        on dw.staff = wp.staff
        and to_char(dw.ts_date, 'Dy') = wp.work_day
),

final as (
    select
        staff,
        ts_date,
        leave_type,
        round((expected_hours - worked_hours)::numeric, 2) as correct_leave_hours,
        entered_by_staff
    from combined
    where total_hours != expected_hours
    order by staff, ts_date
)

select * from final
