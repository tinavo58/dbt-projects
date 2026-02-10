-- This test fails if any records are found. 
-- If it passes, it means there are no variance between payroll and timesheets
select * from {{ ref('fct_2a_payroll_variance_casual') }}
