{{ config(materialized='table') }}

WITH categorized_courses AS (
    SELECT
        state_name,
        course_name,
        CASE
            WHEN LOWER(course_category) = 'long term engagement' THEN 'long_term'
            ELSE 'short_term'
        END AS course_type
    FROM intermediate.participant_impact
    WHERE
        state_name IS NOT NULL -- Ensure state_name is present
),
both_training_participants AS (
    SELECT
        state_name,
        course_name,
        course_type 
    FROM categorized_courses
    GROUP BY state_name, course_name, course_type
)
SELECT
    p.state_name,
    p.course_name,
    p.course_type,
    iso."ISO Code" as iso_code
FROM 
    both_training_participants p
LEFT JOIN 
    prod.india_states_iso_codes iso
ON 
    LOWER(p.state_name) = LOWER(iso."State Name")
ORDER BY 
    p.state_name, p.course_name, p.course_type

