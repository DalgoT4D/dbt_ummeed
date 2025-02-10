{{ config(materialized='table') }}


WITH categorized_data AS (
    SELECT
        start_date AS training_month,
        CASE 
            WHEN LOWER(course_category) = 'long term engagement' THEN 'long_term'
            ELSE 'short_term'
        END AS course_type,
        course_name
    FROM intermediate.participant_impact
)
SELECT
    training_month,
    COUNT(DISTINCT CASE WHEN course_type = 'long_term' THEN course_name END) AS long_term_courses,
    COUNT(DISTINCT CASE WHEN course_type = 'short_term' THEN course_name END) AS short_term_courses
FROM
    categorized_data
GROUP BY
    training_month
ORDER BY
    training_month



