with staff_base as (
    select * from {{ source('public', 'eh_staff') }}
),

current_exceptions as (
    select * from {{ ref('int_daily_exceptions_base') }}
),

-- Adding logic for relevant staff details
selected_staff_details as (
    select
        first_name || ' ' || surname as staff,
        upper(left(trim(split_part(primary_location, ' - ', 1)), 3)) as site
    from staff_base
    where pay_schedule ilike '%weekly%' -- Weekly FT staff
),

leave_data as (
    select
        staff,
        sum(duration) as leave_taken
    from {{ ref('leave_requests') }}
    group by 1
),

worked_hours as (
    select
        week_ending,
        staff,
        site,
        count(staff) as shifts_worked,
        sum(total_hours) as total_hours
    from current_exceptions
    where role_access ilike '%fulltime%'
    group by 1, 2, 3
),

final as (
    select
        coalesce(
            w.week_ending,
            (select max(week_ending) from {{ source('public', 'exception_report') }})
        ) as week_ending,
        s.staff,
        s.site,
        w.shifts_worked,
        w.total_hours,
        l.leave_taken,
        null as phnw, -- PHNW, needs dynamic logic later
        coalesce(w.total_hours, 0) + coalesce(l.leave_taken, 0) as all_hours
    from selected_staff_details as s
    left join worked_hours as w
        using (staff)
    left join leave_data as l
        using (staff)
    where
        coalesce(w.total_hours, 0) + coalesce(l.leave_taken, 0) < 37
        and not (s.staff ilike 'Dallas%' and w.total_hours between 29 and 30)
)

select * from final
