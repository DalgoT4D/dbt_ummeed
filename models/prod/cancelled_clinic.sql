{{ config(materialized='table') }}

WITH clinic_data AS (
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
        consultation_date AS event_date,
        consultation_type AS visit_type,
        unit,
        visit_no,
        billed_status,
        sponsor_name,
        plan,
        associate_company,
        registered_patient_id,
        registered_patient_age,
        date_of_birth,
        registered_patient_gender,
        diagnosis,
        registered_mobile_no,
        calculated_age::TEXT,
        age_group,
        who_brought_the_child,
        location_category,
        pat_idn_no,
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
        year,
        financial_year,
        month,
        quarter,
        'clinic' AS source,

        -- Appointment-only columns as NULLs
        'Checked In' AS event_status,
        NULL::TEXT AS cancel_code, -- noqa: CV11
        NULL::TEXT AS cancelled_by,
        NULL::TEXT AS cancellation_reason,
        NULL::TIMESTAMP AS event_valid_from,
        NULL::TIMESTAMP AS event_valid_to,
        NULL::TEXT AS room_no,
        NULL::TEXT AS address,
        NULL::TEXT AS city,
        NULL::TEXT AS created_by

    FROM {{ ref('clinic_bay_mgmt') }}
),

filtered_appointments AS (
    SELECT
        NULL::TEXT AS s_no,
        mrno,
        patient_name,
        patient_age,
        NULL::VARCHAR AS patient_gender,
        mobile_no,
        department,
        doctor,
        doctor_level,
        event_valid_to AS event_date,
        'Appointment' AS visit_type,
        unit,
        NULL::VARCHAR AS visit_no,
        NULL::VARCHAR AS billed_status,
        NULL::VARCHAR AS sponsor_name,
        NULL::VARCHAR AS plan,
        NULL::VARCHAR AS associate_company,
        NULL::VARCHAR AS registered_patient_id,
        NULL::VARCHAR AS registered_patient_age,
        dob AS date_of_birth,
        NULL::TEXT AS registered_patient_gender,
        NULL::TEXT AS diagnosis,
        NULL::VARCHAR AS registered_mobile_no,
        NULL::TEXT AS calculated_age,
        NULL::TEXT AS age_group,
        NULL::TEXT AS who_brought_the_child,
        NULL::TEXT AS location_category,
        NULL::TEXT AS pat_idn_no,
        NULL::TEXT AS plan_name,
        isprocessed AS is_processed,
        updated_date,
        created_date AS inserted_date,
        NULL::TEXT AS identity_type,
        NULL::TEXT AS patient_income,
        NULL::TEXT AS registration_type,
        NULL::TEXT AS service_center_name,
        NULL::TEXT AS consultation_category,
        NULL::TEXT AS dep_consult_category,
        NULL::TEXT AS dep_shortened,
        year,
        financial_year,
        month,
        quarter,
        'appointment' AS source,

        -- Appointment-specific fields
        CASE 
            WHEN LOWER(event_status) = 'cancel' THEN 'Cancel'
            WHEN LOWER(event_status) = 'no_show' THEN 'No Show'
            ELSE event_status
        END AS event_status,
        cancel_code,
        cancelled_by,
        cancellation_reason,
        event_valid_from,
        event_valid_to,
        room_no,
        address,
        city,
        created_by

    FROM {{ ref('appointment_details') }}
    WHERE LOWER(event_status) IN ('cancel', 'no_show')
)

SELECT * FROM clinic_data
UNION ALL
SELECT * FROM filtered_appointments
