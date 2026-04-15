-- check birthday leave taken if within the 2 weeks span
WITH dob_data AS (
    select
    	first_name || ' ' || surname as staff,
	    date_of_birth,
	    case 
		    when primary_location ilike '%overhead%' then 'Corporate'
	    	else UPPER(left(primary_location, 3))
	    end site
    FROM {{ source('public', 'eh_staff') }}
),
candidates AS (
    SELECT 
        lr.*,
        d.date_of_birth,
        d.site,
        -- Generate the 3 possible birthdays relative to the leave year
        (d.date_of_birth + ((extract(year from lr.end_date) - extract(year from d.date_of_birth)) * interval '1 year'))::date as bday_current,
        (d.date_of_birth + ((extract(year from lr.end_date) - extract(year from d.date_of_birth) - 1) * interval '1 year'))::date as bday_prev,
        (d.date_of_birth + ((extract(year from lr.end_date) - extract(year from d.date_of_birth) + 1) * interval '1 year'))::date as bday_next
    FROM {{ ref('leave_requests') }} lr
    LEFT JOIN dob_data d USING (staff)
    WHERE lr.leave_type ILIKE '%birthday%'
)
SELECT 
    site,
    staff,
    end_date as leave_taken_date,
    date_of_birth,
    -- LOGIC: Find which of the 3 birthdays provides the smallest gap
    CASE 
        -- Check if "Current Year" is the closest
        WHEN ABS(end_date - bday_current) <= ABS(end_date - bday_prev) 
             AND ABS(end_date - bday_current) <= ABS(end_date - bday_next) 
             THEN end_date - bday_current
        -- Check if "Previous Year" is the closest
        WHEN ABS(end_date - bday_prev) < ABS(end_date - bday_next) 
             THEN end_date - bday_prev
        -- Otherwise, it must be "Next Year"
        ELSE end_date - bday_next
    END AS days_from_birthday,
    -- The simple Check Flag
    CASE 
        -- We repeat the logic slightly here, or you can wrap it in another CTE
        WHEN LEAST(ABS(end_date - bday_current), ABS(end_date - bday_prev), ABS(end_date - bday_next)) <= 14 
        THEN TRUE 
        ELSE FALSE 
    END as within_2_weeks
FROM candidates
order by site, leave_taken_date
