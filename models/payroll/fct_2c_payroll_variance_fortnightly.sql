with payruns as (
    select * from {{ source('public', 'payruns_fortnightly') }}
),

latest_weekending as (
    select max(weekending) as max_we from payruns
),

last_payrun as (
    select
        employee_id
        , employee
        , total_hours as lastPay_hrs
        , gross as lastPay_gross
        , post_tax_deduction as lastPay_postDeduction
        , net_pay as lastPay_net
    from payruns
    where
        weekending = (select max_we from latest_weekending) - INTERVAL '14 days'
),

current_payrun as (
    SELECT
        employee_id
        , employee
        , total_hours as thisPay_hrs
        , gross as thisPay_gross
        , post_tax_deduction as thisPay_postDeduction
        , net_pay as thisPay_net
    from payruns
    where
        weekending = (select max_we from latest_weekending)
)

select
    coalesce(c.employee_id, l.employee_id) as employee_id,
    coalesce(c.employee, l.employee) as employee,
    l.lastPay_hrs
    , c.thisPay_hrs
    , (coalesce(c.thisPay_hrs, 0) - coalesce(l.lastPay_hrs, 0)) as diff_hrs
    , l.lastPay_gross
    , c.thisPay_gross
    , (coalesce(c.thisPay_gross, 0) - coalesce(l.lastPay_gross, 0)) as diff_gross
    , l.lastPay_postDeduction
    , c.thisPay_postDeduction
    , (coalesce(c.thisPay_postDeduction, 0) - coalesce(l.lastPay_postDeduction, 0)) as diff_postDeduction
    , l.lastPay_net
    , c.thisPay_net
    , (c.thisPay_net - l.lastPay_net) as diff_net
from current_payrun c
full outer join last_payrun l
on c.employee_id = l.employee_id
order by c.employee
