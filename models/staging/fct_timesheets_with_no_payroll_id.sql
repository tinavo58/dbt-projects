with current_ts as (
    select * from {{ ref('payroll_export') }}
),

final as (
    select
        employee_id,
        staff,
        -- remove `Man` location code so that cleaners' ts get costed to their primary CC
        case
            when location_code = 'Man' then null
            else location_code
        END as location_external_id,
        ts_date as date,
        ts_start as start_time,
        ts_end as end_time,
        CASE
            when ts_mealbreak = '00:00' then null
            else ts_mealbreak
        end as break_duration
    from current_ts
    where employee_id is null
        and staff not in (select staff from {{ ref('fct_labour_hire')}})
)

select * from final
