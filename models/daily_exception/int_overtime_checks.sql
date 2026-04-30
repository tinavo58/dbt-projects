with export_data as (
    select * from {{ source('public', 'daily_exception_export') }}
),

latest_week as (
    select max(week_ending) as max_week from export_data
),

ot_rules as (
    select
        week_ending,
        site,
        staff,
        ts_date,
        ts_start,
        ts_end,
        total_hours,
        sum(pay_rule_units) as ot_hours,
        sum(pay_rule_cost) as ot_cost
    from export_data
    where week_ending = (select max_week from latest_week)
        and site != 'MAN'
        and (
            pay_rule ilike '% OT%'
            or pay_rule ilike '% first 2%'
            or pay_rule ilike '% after 2%'
            or pay_rule ilike '%weekly%'
            or pay_rule ilike '%less than%'
            or pay_rule ilike '%meal%'
        )
    group by
        week_ending, site, staff,
        ts_date, ts_start, ts_end, total_hours
)

select
    week_ending,
    'Overtime' as check_type,
    site,
    staff,
    null::text as role_access,
    ts_date,
    ts_start,
    ts_end,
    total_hours,
    null::interval as ts_mealbreak,
    null::interval as ts_restbreak,
    round(ot_hours::numeric, 2) as ot_hours,
    round(ot_cost::numeric, 2) as ot_cost
from ot_rules
