with current_timesheets as (
    select * from {{ ref('payroll_export') }}
),

deputy_staff as (
    select * from {{ source('public', 'deputy_staff') }}
),

nw_staff as (
    select staff, access_role
    from deputy_staff
    where access_role ilike '%national%'
),

nw_timesheets as (
    select
        staff,
        ts_date as date,
        access_role,
        location_name as area,
        ts_start as start_time,
        ts_end as end_time,
        ts_total_hours as total_time,
        ts_mealbreak as meal_break
    from current_timesheets
    join nw_staff using (staff)
    order by staff, ts_date
)

select * from nw_timesheets
