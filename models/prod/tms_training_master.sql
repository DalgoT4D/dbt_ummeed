{{ config(materialized='table') }}


SELECT
    pid,
    participant, 
    primary_contact,
    state_name as state, 
    iso."ISO Code" as iso_code,
    gender,
    course_name,
    course_category,
    course_short_name,
    organisation_name,
    reg_attending_program,
    start_date,
    TO_CHAR(start_date, 'Mon') AS month,
    EXTRACT(YEAR FROM start_date) AS year,
    
    -- Calculate Financial Year
    CASE 
        WHEN EXTRACT(MONTH FROM start_date) >= 4 THEN 
            CONCAT(EXTRACT(YEAR FROM start_date), '-', EXTRACT(YEAR FROM start_date) + 1)
        ELSE 
            CONCAT(EXTRACT(YEAR FROM start_date) - 1, '-', EXTRACT(YEAR FROM start_date))
    END AS financial_year

FROM intermediate.participant_impact as p
LEFT JOIN 
    prod.india_states_iso iso
ON 
    LOWER(p.state_name) = LOWER(iso."State")





