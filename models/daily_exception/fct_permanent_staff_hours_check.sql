with staff_base as (
    select * from {{ source('public', 'eh_staff') }}
),

current_exceptions as (
    select * from {{ ref('int_daily_exceptions_base') }}
),

-- Adding logic for relevant staff details
-- TODO: will use employee_id to link
selected_staff_details as (
    select
        first_name || ' ' || surname as staff,
        hours_per_week,
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
        SUM(case WHEN to_char(ts_date, 'Dy') = 'Mon' THEN total_hours ELSE null END) as Mon,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Tue' THEN total_hours ELSE null END) as Tue,
        SUM(case WHEN to_char(ts_date, 'Dy') = 'Wed' THEN total_hours ELSE null END) as Wed,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Thu' THEN total_hours ELSE null END) as Thu,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Fri' THEN total_hours ELSE null END) as Fri,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Sat' THEN total_hours ELSE null END) as Sat,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Sun' THEN total_hours ELSE null END) as Sun,
        count(staff) as shifts_worked,
        sum(total_hours) as total_hours
    from current_exceptions
    group by 1, 2, 3
),

latest_we as (
    select max(week_ending) as we from {{ source('public', 'exception_report') }}
),

final as (
    select
        coalesce(w.week_ending, (select we from latest_we)) as week_ending,
        s.site, -- move site before staff
        s.staff,
        w.Mon, w.Tue, w.Wed, w.Thu, w.Fri, w.Sat, w.Sun,
        w.shifts_worked,
        w.total_hours,
        l.leave_taken,
        null as phnw, -- PHNW, needs dynamic logic later
        coalesce(w.total_hours, 0) + coalesce(l.leave_taken, 0) as all_hours, -- update when there's PHNW
        s.hours_per_week,
        coalesce(w.total_hours, 0) + coalesce(l.leave_taken, 0) - s.hours_per_week as hours_diff
    from selected_staff_details as s
    left join worked_hours as w
        using (staff)
    left join leave_data as l
        using (staff)
    where
        coalesce(w.total_hours, 0) + coalesce(l.leave_taken, 0) < s.hours_per_week
    order by site, all_hours desc
)

select * from final
