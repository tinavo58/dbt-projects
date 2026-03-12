with payruns as (
    select * from {{ source('public', 'payruns_cas') }}
),

latest_week as (
    select max(weekending) as max_we from payruns
)

select
    *
from payruns
where weekending = (select max_we from latest_week)
