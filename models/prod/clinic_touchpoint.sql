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
        rp.patient_age, 
        rp.registration_type,
        rp.updated_date,
        pi.course_name,
        pi.course_category,
        pi.reg_attending_program
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
