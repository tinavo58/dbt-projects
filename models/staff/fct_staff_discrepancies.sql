WITH eh AS (
    SELECT 
        employee_id::text AS employee_id, -- Cast to text to avoid type mismatch errors later
        TRIM(first_name || ' ' || surname) AS staff_name
    FROM {{ source('public', 'eh_staff')}}
    where pay_schedule not ilike '% FN %'
),
deputy AS (
    SELECT 
        payroll_id::text AS payroll_id,
        staff AS staff_name,
        access_role
    FROM {{ source('public', 'deputy_staff')}}
    -- Filter out system accounts first
    WHERE access_role NOT ILIKE ALL(ARRAY['sys%', 'sup%', 'loc%'])
       OR access_role IS NULL
),
-- STEP 1: Get ALL EH Staff and try to find their Deputy match (by ID OR Name)
eh_matches AS (
    SELECT
        eh.employee_id AS eh_id,
        eh.staff_name AS eh_name,
        d.payroll_id AS deputy_id,
        d.staff_name AS deputy_name,
        d.access_role
    FROM eh
    LEFT JOIN deputy d
        ON eh.employee_id = d.payroll_id 
        OR eh.staff_name ILIKE d.staff_name
),
-- STEP 2: Find Deputy staff who have NO match in EH whatsoever
deputy_only AS (
    SELECT
        NULL AS eh_id,
        NULL AS eh_name,
        d.payroll_id AS deputy_id,
        d.staff_name AS deputy_name,
        d.access_role
    FROM deputy d
    WHERE NOT EXISTS (
        SELECT 1 FROM eh 
        WHERE eh.employee_id = d.payroll_id 
           OR eh.staff_name ILIKE d.staff_name
    )
),
-- STEP 3: Combine them together
reconciliation AS (
    SELECT * FROM eh_matches
    UNION ALL
    SELECT * FROM deputy_only
)
-- FINAL SELECT: Apply the Status logic
SELECT
    eh_id, 
    eh_name, 
    deputy_id, 
    deputy_name, 
    access_role,
    CASE
        WHEN eh_id IS NULL THEN 'Missing in EH (Only in Deputy)'
        WHEN deputy_name IS NULL THEN 'Missing in Deputy (Only in EH)'
        WHEN eh_id = deputy_id THEN 'Perfect Match'
        WHEN deputy_id IS NULL THEN 'Match by Name (Deputy missing ID)'
        ELSE 'Match by Name (IDs are different)'
    END AS status
FROM reconciliation
where coalesce(eh_id::integer, 0) <> coalesce(deputy_id::integer, 0)
ORDER BY status, eh_name
