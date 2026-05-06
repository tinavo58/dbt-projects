with staff_base as (
    select * from {{ source('public', 'eh_staff') }}
),

current_exceptions as (
    select * from {{ ref('int_daily_exceptions_base') }}
),

selected_staff_details as (
    select
        first_name || ' ' || surname as staff,
        hours_per_week,
        upper(left(trim(split_part(primary_location, ' - ', 1)), 3)) as site,
        residential_state
    from staff_base
    where pay_schedule ilike '%weekly%'
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
    group by 1, 2
),

latest_we as (
    select max(week_ending) as we from {{ source('public', 'exception_report') }}
),

public_holidays as (
    select * from {{ ref('dim_public_holidays') }}
),

work_patterns as (
    select * from {{ ref('dim_staff_work_patterns') }}
),

parental_leave as (
    select distinct staff
    from {{ ref('leave_requests') }}
    where leave_type ilike '%parental%'
),

staff_phnw as (
    select
        s.staff,
        sum(
            case
                when s.hours_per_week >= 38 then 7.6
                else wp.daily_hours
            end
        ) as phnw
    from selected_staff_details as s
    cross join public_holidays as ph
    left join work_patterns as wp
        on s.staff = wp.staff
        and to_char(ph.holiday_date, 'Dy') = wp.work_day
    where
        ph.holiday_date >= (select we from latest_we) - 6
        and ph.holiday_date <= (select we from latest_we)
        and extract(isodow from ph.holiday_date) between 1 and 5
        and (ph.state = 'ALL' or ph.state = s.residential_state)
        and s.staff not in (select staff from parental_leave)
        and (
            s.hours_per_week >= 38
            or wp.staff is not null -- part-timer works on this day
        )
    group by 1
),

final as (
    select
        coalesce(w.week_ending, (select we from latest_we)) as week_ending,
        s.site,
        s.staff,
        w.Mon, w.Tue, w.Wed, w.Thu, w.Fri, w.Sat, w.Sun,
        w.shifts_worked,
        w.total_hours,
        l.leave_taken,
        coalesce(sp.phnw, 0) as phnw,
        coalesce(w.total_hours, 0) + coalesce(l.leave_taken, 0) + coalesce(sp.phnw, 0) as all_hours,
        s.hours_per_week,
        coalesce(w.total_hours, 0) + coalesce(l.leave_taken, 0) + coalesce(sp.phnw, 0) - s.hours_per_week as hours_diff
    from selected_staff_details as s
    left join worked_hours as w
        using (staff)
    left join leave_data as l
        using (staff)
    left join staff_phnw as sp
        using (staff)
    where
        coalesce(w.total_hours, 0) + coalesce(l.leave_taken, 0) + coalesce(sp.phnw, 0) < s.hours_per_week
    order by site, all_hours desc
)

select * from final
