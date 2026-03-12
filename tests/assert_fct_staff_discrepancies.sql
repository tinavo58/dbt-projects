-- check if there are any discrepancies between Deputy and EH platforms
select *
from {{ ref('fct_staff_discrepancies')}}