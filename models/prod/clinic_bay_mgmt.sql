{{ config(materialized='table') }}

SELECT
    s_no,
    mrno,
    patient_name,
    patient_age,
    patient_gender,
    mobile_no,
    department,
    doctor,
    doctor_level,
    consultation_date,
    consultation_type,
    unit,
    visit_no,
    billed_status,
    invoice_no,
    queue_generation_time,
    queue_no,
    start_consultation_time,
    end_consultation_time,
    sponsor_name,
    plan,
    associate_company,
    registered_patient_id,
    registered_patient_age,
    date_of_birth,
    registered_patient_gender,
    diagnosis,
    registered_mobile_no,
    calculated_age,
    age_group,
    parent_guardian_phone,
    parent_guardian_age,
    parent_guardian_city,
    parent_guardian_district,
    parent_guardian_state,
    parent_guardian_country,
    who_brought_the_child,
    location_category,
    pat_idn_no,
    guardian_pin,
    plan_name,
    is_processed,
    updated_date,
    inserted_date,
    identity_type,
    patient_income,
    registration_type,
    service_center_name,
    consultation_category, 
    dep_consult_category, 
    dep_shortened,
    EXTRACT(YEAR FROM consultation_date) AS year,
    -- Calculate Financial Year
    CASE 
        WHEN EXTRACT(MONTH FROM consultation_date) >= 4 
            THEN CONCAT(
                EXTRACT(YEAR FROM consultation_date), '-', 
                EXTRACT(YEAR FROM consultation_date) + 1
            )
        ELSE CONCAT(
            EXTRACT(YEAR FROM consultation_date) - 1, '-', 
            EXTRACT(YEAR FROM consultation_date)
        )
    END AS financial_year,
    -- Month
    TO_CHAR(consultation_date, 'Month') AS month,
    -- Quarter
    CASE 
        WHEN EXTRACT(MONTH FROM consultation_date) BETWEEN 1 AND 3 
            THEN 'Q4'
        WHEN EXTRACT(MONTH FROM consultation_date) BETWEEN 4 AND 6 
            THEN 'Q1'
        WHEN EXTRACT(MONTH FROM consultation_date) BETWEEN 7 AND 9 
            THEN 'Q2'
        WHEN EXTRACT(MONTH FROM consultation_date) BETWEEN 10 AND 12 
            THEN 'Q3'
    END AS quarter

FROM {{ ref('clinic_data') }}
WHERE 
    TRIM(patient_name) <> 'Dummy Ummeed'
    AND consultation_type NOT IN ('Internal Review', 'Phone/Email Query', 'UPPA Fees')
