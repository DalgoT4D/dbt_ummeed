{{ config(materialized='table') }}

WITH cte AS (
    SELECT 
        rp.mrno,
        pi.primary_contact,
        rp.mobile_no,
        rp.father_phone,
        rp.mother_phone,
        pi.state_name,
        pi.start_date,
        rp.registered_patient_age, 
        rp.registration_type,
        rp.updated_date,
        pi.course_name,
        pi.course_category,
        pi.reg_attending_program,
        -- Calculate Financial Year
        CASE 
            WHEN EXTRACT(MONTH FROM start_date) >= 4 
            THEN CONCAT(EXTRACT(YEAR FROM start_date), '-', 
                        EXTRACT(YEAR FROM start_date) + 1)
            ELSE CONCAT(EXTRACT(YEAR FROM start_date) - 1, '-', 
                        EXTRACT(YEAR FROM start_date))
        END AS financial_year,
        EXTRACT(MONTH FROM start_date) AS month
    FROM 
        intermediate.participant_impact AS pi
    LEFT JOIN 
        intermediate.registered_patient AS rp
    ON 
        rp.mobile_no = pi.primary_contact
)
SELECT 
    DISTINCT ON (mrno) *
FROM 
    cte
WHERE 
    mrno IS NOT NULL
