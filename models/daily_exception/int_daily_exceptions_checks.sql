with base_exceptions as (
    select * from {{ ref('int_daily_exceptions_base') }}
),

auto_closed as (
    select
        week_ending,
        'Auto Closed Shift' as check_type,
        site,
        staff,
        role_access,
        ts_date,
        ts_start,
        ts_end,
        total_hours,
        ts_mealbreak,
        ts_restbreak
    from base_exceptions
    where validation_flag = 'auto closed'
),

min_engagement as (
    select
        week_ending,
        'Min Engagement' as check_type,
        site,
        staff,
        role_access,
        ts_date,
        ts_start,
        ts_end,
        total_hours,
        ts_mealbreak,
        ts_restbreak
    from base_exceptions
    where (staff, ts_date) IN (
        select staff, ts_date
        from base_exceptions
        where role_access ilike '%casual%'
        group by staff, ts_date
        having sum(total_hours) < 4
    )
),

long_rest as (
    select
        week_ending,
        'Long Rest Break' as check_type,
        site,
        staff,
        role_access,
        ts_date,
        ts_start,
        ts_end,
        total_hours,
        ts_mealbreak,
        ts_restbreak
    from base_exceptions
    where ts_restbreak > INTERVAL '25 minutes'
        and not (total_hours > 8.6 and ts_restbreak between interval '25 minutes' and INTERVAL '35 minutes')
),

long_meal as (
    select
        week_ending,
        'Long Meal Break' as check_type,
        site,
        staff,
        role_access,
        ts_date,
        ts_start,
        ts_end,
        total_hours,
        ts_mealbreak,
        ts_restbreak
    from base_exceptions
    where ts_mealbreak > INTERVAL '35 minutes'
),

short_meal as (
    select
        week_ending,
        'Missed/Short Meal Break' as check_type,
        site,
        staff,
        role_access,
        ts_date,
        ts_start,
        ts_end,
        total_hours,
        ts_mealbreak,
        ts_restbreak
    from base_exceptions
    where ts_mealbreak < INTERVAL '25 minutes' and total_hours > 5.17
),

unioned_checks as (
    select * from auto_closed
    union all
    select * from min_engagement
    union all
    select * from long_rest
    union all
    select * from long_meal
    union all
    select * from short_meal
)

select * from unioned_checks
