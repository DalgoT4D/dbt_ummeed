{{ config(materialized='table') }}


SELECT
    p.pid,
    p.participant, 
    p.primary_contact,
    p.state_name AS state, 
    iso."ISO Code" AS iso_code,
    p.gender,
    p.course_name,
    p.course_category,
    p.course_short_name,
    p.organisation_name,
    COALESCE(p.reg_attending_program, 'Not Available') AS reg_attending_program,
    p.start_date,
    nr.department,
    EXTRACT(YEAR FROM p.start_date) AS year,
    
    -- Calculate Financial Year
    CASE 
        WHEN EXTRACT(MONTH FROM start_date) >= 4
            THEN 
                CONCAT(EXTRACT(YEAR FROM start_date), '-', EXTRACT(YEAR FROM start_date) + 1)
        ELSE 
            CONCAT(EXTRACT(YEAR FROM start_date) - 1, '-', EXTRACT(YEAR FROM start_date))
    END AS financial_year,
    TO_CHAR(start_date, 'Month') AS month

FROM intermediate.participant_impact AS p
LEFT JOIN 
    prod.india_states_iso AS iso
    ON 
        LOWER(p.state_name) = LOWER(iso."State")
LEFT JOIN 
    {{ ref('no_registration') }} AS nr
    ON 
        p.course_short_name = nr.course_short_name
