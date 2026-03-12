with latest_week as (
    select max(weekending) as max_weekending
    from {{ source('public', 'payruns_weekly_ft') }}
)
select * 
from {{ source('public', 'payruns_weekly_ft') }}
where weekending = (select max_weekending from latest_week)
