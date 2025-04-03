{{ config(materialized='table') }}

SELECT 
    patient_age,
    city,
    mrno,
    dob,
    patient_gender,
    unit,
    room_no,
    address,
    event_id,
    mobile_no,
    created_by,
    doctor,
    department,
    created_date,
    event_status,
    isprocessed,
    patient_name,
    slot_end_time,
    updated_date,
    cancel_code,
    cancelled_by,
    cancellation_reason,
    slot_start_time,
    event_valid_from,
    event_valid_to,

    EXTRACT(YEAR FROM event_valid_to) AS year,
    -- Calculate Financial Year
    CASE 
        WHEN EXTRACT(MONTH FROM event_valid_to) >= 4 
            THEN CONCAT(EXTRACT(YEAR FROM event_valid_to), '-', EXTRACT(YEAR FROM event_valid_to) + 1)
        ELSE CONCAT(EXTRACT(YEAR FROM event_valid_to) - 1, '-', EXTRACT(YEAR FROM event_valid_to))
    END AS financial_year,
    -- Month
    TO_CHAR(event_valid_to, 'Month') AS month,
    -- Quarter
    CASE 
        WHEN EXTRACT(MONTH FROM event_valid_to) BETWEEN 1 AND 3 
            THEN 'Q4'
        WHEN EXTRACT(MONTH FROM event_valid_to) BETWEEN 4 AND 6 
            THEN 'Q1'
        WHEN EXTRACT(MONTH FROM event_valid_to) BETWEEN 7 AND 9 
            THEN 'Q2'
        WHEN EXTRACT(MONTH FROM event_valid_to) BETWEEN 10 AND 12 
            THEN 'Q3'
    END AS quarter
FROM {{ ref('appointment_data') }}
