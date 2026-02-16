with exceptions as (
    select * from {{ ref('int_daily_exceptions_base') }}
),

deputy_staff as (
    select * from {{ source('public', 'deputy_staff') }}
),

nw_staff as (
    select staff from deputy_staff where access_role ilike '%national%'
),

nw_timesheets as (
    select
        staff as display_name,
        ts_date as timesheet_date,
        role_access as access_level_name,
        site as area,
        ts_start as timesheet_start_time,
        ts_end as timesheet_end_time,
        total_hours as timesheet_total_time,
        ts_mealbreak as timesheet_meal_break
    from exceptions
    where staff in (select staff from nw_staff)
)

select * from nw_timesheets
