{{ config(materialized='table') }}

WITH clinic_data AS (
    SELECT 
        s_no,
        mrno,
        patient_name,
        CAST(patient_age AS TEXT) AS patient_age,
        patient_gender,
        mobile_no,
        department,
        doctor,
        consultation_date AS event_date,
        consultation_type AS visit_type,
        unit,
        visit_no,
        billed_status,
        invoice_no,
        CAST(queue_generation_time AS TEXT) AS queue_generation_time,
        queue_no,
        CAST(start_consultation_time AS TEXT) AS start_consultation_time,
        CAST(end_consultation_time AS TEXT) AS end_consultation_time,
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
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
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
        NULL::TEXT AS slot_start_time,
        NULL::TEXT AS slot_end_time,
        NULL::TIMESTAMP AS event_valid_from,
        NULL::TIMESTAMP AS event_valid_to,
        NULL::TIMESTAMP AS appointment_created_date,
        NULL::TIMESTAMP AS appointment_updated_date,
        NULL::TEXT AS room_no,
        NULL::TEXT AS address,
        NULL::TEXT AS city,
        NULL::TEXT AS created_by

    FROM {{ ref('clinic_bay_mgmt') }}
),

filtered_appointments AS (
    SELECT
        NULL::NUMERIC(38,9) AS s_no,
        CAST(mrno AS VARCHAR) AS mrno,
        patient_name,
        CAST(patient_age AS TEXT) AS patient_age,
        NULL::VARCHAR AS patient_gender,
        CAST(mobile_no AS VARCHAR) AS mobile_no,
        CAST(department AS VARCHAR) AS department,
        CAST(doctor AS VARCHAR) AS doctor,
        event_valid_to AS event_date,
        'Appointment' AS visit_type,
        unit,
        NULL::VARCHAR AS visit_no,
        NULL::VARCHAR AS billed_status,
        NULL::VARCHAR AS invoice_no,
        NULL::TEXT AS queue_generation_time,
        NULL::VARCHAR AS queue_no,
        CAST(slot_start_time AS TEXT) AS slot_start_time,
        CAST(slot_end_time AS TEXT) AS slot_end_time,
        NULL::VARCHAR AS sponsor_name,
        NULL::VARCHAR AS plan,
        NULL::VARCHAR AS associate_company,
        NULL::NUMERIC(38,9) AS registered_patient_id,
        NULL::NUMERIC(38,9) AS registered_patient_age,
        dob AS date_of_birth,
        NULL::TEXT AS registered_patient_gender,
        NULL::TEXT AS diagnosis,
        NULL::TEXT AS registered_mobile_no,
        NULL::FLOAT8 AS calculated_age,
        NULL::TEXT AS age_group,
        NULL::TEXT AS parent_guardian_phone,
        NULL::TEXT AS parent_guardian_age,
        NULL::TEXT AS parent_guardian_city,
        NULL::TEXT AS parent_guardian_district,
        NULL::TEXT AS parent_guardian_state,
        NULL::TEXT AS parent_guardian_country,
        NULL::TEXT AS who_brought_the_child,
        NULL::TEXT AS location_category,
        NULL::TEXT AS pat_idn_no,
        NULL::TEXT AS guardian_pin,
        NULL::TEXT AS plan_name,
        isprocessed AS is_processed,
        updated_date,
        created_date AS inserted_date,
        NULL::TEXT AS identity_type,
        NULL::TEXT AS patient_income,
        NULL::TEXT AS registration_type,
        NULL::TEXT AS service_center_name,
        NULL::VARCHAR AS _airbyte_raw_id,
        NULL::TIMESTAMPTZ AS _airbyte_extracted_at,
        NULL::JSONB AS _airbyte_meta,
        NULL::TEXT AS consultation_category,
        NULL::TEXT AS dep_consult_category,
        NULL::TEXT AS dep_shortened,
        EXTRACT(YEAR FROM event_valid_to) AS year,
        CASE
            WHEN EXTRACT(MONTH FROM event_valid_to) >= 4 THEN CONCAT(EXTRACT(YEAR FROM event_valid_to), '-', EXTRACT(YEAR FROM event_valid_to)+1)
            ELSE CONCAT(EXTRACT(YEAR FROM event_valid_to)-1, '-', EXTRACT(YEAR FROM event_valid_to))
        END AS financial_year,
        TO_CHAR(event_valid_to, 'Month') AS month,
        CASE
            WHEN EXTRACT(MONTH FROM event_valid_to) BETWEEN 1 AND 3 THEN 'Q4'
            WHEN EXTRACT(MONTH FROM event_valid_to) BETWEEN 4 AND 6 THEN 'Q1'
            WHEN EXTRACT(MONTH FROM event_valid_to) BETWEEN 7 AND 9 THEN 'Q2'
            ELSE 'Q3'
        END AS quarter,
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
        CAST(slot_start_time AS TEXT) AS slot_start_time,
        CAST(slot_end_time AS TEXT) AS slot_end_time,
        CAST(event_valid_from AS TIMESTAMP) AS event_valid_from,
        CAST(event_valid_to AS TIMESTAMP) AS event_valid_to,
        CAST(created_date AS TIMESTAMP) AS appointment_created_date,
        CAST(updated_date AS TIMESTAMP) AS appointment_updated_date,
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
