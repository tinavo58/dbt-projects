with regular_checks as (
    select
        *,
        null::numeric as ot_hours,
        null::numeric as ot_cost
    from {{ ref('int_daily_exceptions_checks') }}
),

overtime_checks as (
    select * from {{ ref('int_overtime_checks') }}
),

final as (
    select * from regular_checks
    union all
    select * from overtime_checks
)

select * from final
