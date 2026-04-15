-- check if birthday leave is within 2 weeks span
select *
from {{ ref('fct_check_birthday_leave') }}
where within_2_weeks = false