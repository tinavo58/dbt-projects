/*
    this test is to check
        - grade 1 & 2 & 4 (casual): no Above Award tag
        - grade 3 & 4 (permanent): Above Award tag assigned
        - grade 3 (casual): Above Award tag assigned
*/
select * from {{ ref('fct_staff_grade_tags_check') }}