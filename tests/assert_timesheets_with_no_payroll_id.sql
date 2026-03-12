-- check if there's staff that worked but not yet completing onboarding
select *
from {{ ref('fct_timesheets_with_no_payroll_id') }}