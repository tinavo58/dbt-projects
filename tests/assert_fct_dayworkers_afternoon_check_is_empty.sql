-- This test fails if any records are found. 
-- If it passes, it means there are no dayworkers working afternoon shifts.
select * from {{ ref('fct_dayworkers_afternoon_check') }}
