/*
    this test is to check
        - grade 1 & 2 & 4 (casual): no Above Award tag
        - grade 3 & 4 (permanent): Above Award tag assigned
        - grade 3 (casual): Above Award tag assigned
*/
with staff as (
    select * from {{ source('public', 'eh_staff') }}
),
final as (
SELECT
    first_name || ' ' || surname as staff,
    pay_rate_template,
    pay_schedule,
    tags,
    CASE 
        WHEN 'Above Award' = ANY(tags) 
             AND NOT (pay_rate_template ILIKE '%grade 3%' OR pay_rate_template ILIKE '%grade 4%')
             THEN 'Error: Low Grade (G1/G2) has Above Award tag'
        WHEN NOT ('Above Award' = ANY(tags))
             AND pay_schedule ILIKE '%weekly%'
             AND (pay_rate_template ILIKE '%grade 3%' OR pay_rate_template ILIKE '%grade 4%')
             THEN 'Error: Weekly G3/G4 is MISSING Above Award tag'
        WHEN 'Above Award' = ANY(tags)
             AND pay_schedule ILIKE '%casual%'
             AND pay_rate_template ILIKE '%grade 4%'
             THEN 'Error: Casual G4 SHOULD NOT have Above Award tag'
    END as error_reason
FROM staff
WHERE 
    -- check G1/G2 (Anything that isn't G3/G4)
    (
        'Above Award' = ANY(tags)
        AND NOT (pay_rate_template ILIKE '%grade 3%' OR pay_rate_template ILIKE '%grade 4%')
    )
    OR 
    -- check Weekly G3/G4 (Must HAVE tag)
    (
        NOT ('Above Award' = ANY(tags)) -- Tag is missing
        AND pay_schedule ILIKE '%weekly%'
        AND (pay_rate_template ILIKE '%grade 3%' OR pay_rate_template ILIKE '%grade 4%')
    )
    OR
    -- check Casual G4 (Must NOT have tag)
    (
        'Above Award' = ANY(tags) -- Tag is present
        AND pay_schedule ILIKE '%casual%'
        AND pay_rate_template ILIKE '%grade 4%'
    )
)
select * from final