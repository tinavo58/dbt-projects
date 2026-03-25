-- check if there were hours worked by labour hire
select *
from {{ ref('fct_national_workforce_export') }}