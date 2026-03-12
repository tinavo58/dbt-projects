-- check if theres any labour hire existing
select *
from {{ ref('fct_labour_hire') }}