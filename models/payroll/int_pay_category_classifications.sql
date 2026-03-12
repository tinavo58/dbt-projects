with all_categories as (
    select distinct pay_category from {{ source('public', 'payruns_cas') }}
    union
    select distinct pay_category from {{ source('public', 'payruns_weekly_ft') }}
)

select
    pay_category,
    CASE
        WHEN pay_category ilike '%allowance%' THEN 'ALLOW'
        WHEN pay_category ilike '%first%' THEN 'OT1.5'
        WHEN pay_category ilike '%after %' THEN 'OT2.0'
        WHEN pay_category ilike '%Sunday%Overtime' THEN 'OT2.0'
        WHEN pay_category ilike '%overtime - less%' THEN 'OT2.0'
        WHEN pay_category = 'Sunday Hours' THEN 'ORD'
        WHEN pay_category ilike '%workcover%' THEN 'WC'
        WHEN pay_category ilike '%ordinary%' THEN 'ORD'
        WHEN pay_category ilike '%shift%' THEN 'ORD'
        ELSE null
    END as classification
from all_categories
