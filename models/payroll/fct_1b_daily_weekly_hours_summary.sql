with current_payrun as (
    select * from {{ ref('int_payruns_weekly_current') }}
),

current_ts as (
    select
        staff, employee_id, ts_date,
        sum(ts_total_hours) as daily_total,
        least(sum(ts_total_hours), 7.6) as estimated_ord,
        greatest(sum(ts_total_hours) - 7.6, 0) as estimated_ot 
    from {{ ref('payroll_export') }}
    group by 1, 2, 3
),

classifications as (
    select * from {{ ref('int_pay_category_classifications') }}
),

daily_hours AS (
    SELECT
        employee_id,
        staff as employee,
        SUM(case WHEN to_char(ts_date, 'Dy') = 'Mon' THEN daily_total ELSE 0 END) as Mon,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Tue' THEN daily_total ELSE 0 END) as Tue,
        SUM(case WHEN to_char(ts_date, 'Dy') = 'Wed' THEN daily_total ELSE 0 END) as Wed,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Thu' THEN daily_total ELSE 0 END) as Thu,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Fri' THEN daily_total ELSE 0 END) as Fri,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Sat' THEN daily_total ELSE 0 END) as Sat,
        SUM(CASE WHEN to_char(ts_date, 'Dy') = 'Sun' THEN daily_total ELSE 0 END) as Sun,
        -- estimate
        sum(estimated_ord) as total_eta_ord,
        sum(estimated_ot) as total_eta_ot
    from current_ts
    group by 1, 2
),

final as (
    select
        p.weekending,
        p.employee_id,
        p.employee,
        SUM(case WHEN c.classification = 'ORD' THEN units ELSE 0 END) as ORD_hrs,
        SUM(case WHEN c.classification = 'OT1.5' THEN units ELSE 0 END) as OT15_hrs,
        SUM(case WHEN c.classification = 'OT2.0' THEN units ELSE 0 END) as OT20_hrs,
        total_eta_ord, total_eta_ot,
        d.Mon, d.Tue, d.Wed, d.Thu, d.Fri, d.Sat, d.Sun
    FROM current_payrun p
    LEFT JOIN classifications c
        on p.pay_category = c.pay_category
    LEFT JOIN daily_hours d
        on p.employee_id = d.employee_id 
        -- and p.employee = d.employee -- comment this out since name does not match
    GROUP BY
        p.weekending,
        p.employee_id,
        p.employee,
        d.Mon, d.Tue, d.Wed, d.Thu, d.Fri, d.Sat, d.Sun,
        total_eta_ord, total_eta_ot
)

select
    weekending,
    employee_id,
    employee,
    ORD_hrs, total_eta_ord, total_eta_ord - ORD_hrs as ORD_diff,
    OT15_hrs, OT20_hrs, total_eta_ot, (total_eta_ot - OT15_hrs - OT20_hrs) as OT_diff,
    Mon, Tue, Wed, Thu, Fri, Sat, Sun
from final
ORDER BY
    OT15_hrs desc,
    OT20_hrs desc,
    ORD_hrs desc
