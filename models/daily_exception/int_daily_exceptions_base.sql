with exceptions as (
    select * from {{ source('public', 'exception_report') }}
),

latest_week as (
    select max(week_ending) as max_week from exceptions
),

base_exceptions as (
    select
        week_ending,
        site,
        staff,
        role_access,
        ts_date,
        ts_start,
        ts_end,
        ts_mealbreak,
        ts_restbreak,
        total_hours,
        case
            when validation_flag ilike '%automatically closed%' then 'auto closed'
            else null
        end as validation_flag
    from exceptions
    where week_ending = (select max_week from latest_week)
    and site <> 'MAN'
)

select * from base_exceptions
