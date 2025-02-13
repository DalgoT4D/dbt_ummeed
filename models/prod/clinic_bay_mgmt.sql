{{ config(materialized='table') }}

WITH clinic_data AS (
    SELECT 
        s_no,
        mrn,
        patient_name,
        age,
        gender,
        mobile_no,
        department,
        doctor,
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
        associate_company
    FROM {{ ref('clinic_data') }}
),

registered_patient AS (
    SELECT 
        id AS registered_patient_id,
        age AS registered_patient_age,
        dob AS date_of_birth,
        mrno AS mrn,  -- Renaming for consistency in the join
        gender AS registered_gender,
        diagnosis,
        mobile_no AS registered_mobile_no,
        pat_idn_no,
        guardian_pin,

        -- Coalescing parent/guardian details
        COALESCE(father_phone, mother_phone) AS parent_guardian_phone,
        COALESCE(father_age, guardian_age) AS parent_guardian_age,
        COALESCE(father_city, mother_city, guardian_city) AS parent_guardian_city,
        COALESCE(father_dist, mother_dist, guardian_dist) AS parent_guardian_district,
        COALESCE(father_state, mother_state, guardian_rstate) AS parent_guardian_state,
        COALESCE(father_country, mother_country) AS parent_guardian_country,

        -- Identifying who brought the child
        CASE 
            WHEN father_phone IS NOT NULL THEN 'Father'
            WHEN father_age IS NOT NULL THEN 'Father'
            WHEN father_city IS NOT NULL THEN 'Father'
            WHEN father_country IS NOT NULL THEN 'Father'
            WHEN father_dist IS NOT NULL THEN 'Father'
            WHEN father_state IS NOT NULL THEN 'Father'
            WHEN mother_city IS NOT NULL THEN 'Mother'
            WHEN mother_country IS NOT NULL THEN 'Mother'
            WHEN mother_dist IS NOT NULL THEN 'Mother'
            WHEN mother_state IS NOT NULL THEN 'Mother'
            WHEN mother_phone IS NOT NULL THEN 'Mother'
            WHEN guardian_pin IS NOT NULL THEN 'Guardian'
            WHEN guardian_city IS NOT NULL THEN 'Guardian'
            WHEN guardian_dist IS NOT NULL THEN 'Guardian'
            WHEN guardian_rstate IS NOT NULL THEN 'Guardian'
            WHEN guardian_age IS NOT NULL THEN 'Guardian'
            ELSE NULL
        END AS who_brought_the_child,

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
        _airbyte_meta
    FROM {{ ref('registered_patient') }}
)

SELECT 
    cd.s_no,
    cd.mrn,
    cd.patient_name,
    cd.age AS patient_age,
    cd.gender AS patient_gender,
    cd.mobile_no AS patient_mobile_no,
    cd.department,
    cd.doctor,
    cd.consultation_date,
    cd.consultation_type,
    cd.unit,
    cd.visit_no,
    cd.billed_status,
    cd.invoice_no,
    cd.queue_generation_time,
    cd.queue_no,
    cd.start_consultation_time,
    cd.end_consultation_time,
    cd.sponsor_name,
    cd.plan,
    cd.associate_company,

    rp.registered_patient_id,
    rp.registered_patient_age,
    rp.date_of_birth,
    rp.registered_gender,
    rp.diagnosis,
    rp.registered_mobile_no,

    -- Coalesced Parent/Guardian Details
    rp.parent_guardian_phone,
    rp.parent_guardian_age,
    rp.parent_guardian_city,
    rp.parent_guardian_district,
    rp.parent_guardian_state,
    rp.parent_guardian_country,
    rp.who_brought_the_child,  -- Specifies whether data came from father, mother, or guardian

    -- Coalesced Patient ID
    rp.pat_idn_no,
    rp.guardian_pin,

    rp.plan_name,
    rp.is_processed,
    rp.updated_date,
    rp.inserted_date,
    rp.identity_type,
    rp.patient_income,
    rp.registration_type,
    rp.service_center_name,
    rp._airbyte_raw_id,
    rp._airbyte_extracted_at,
    rp._airbyte_meta

FROM clinic_data cd
LEFT JOIN registered_patient rp
ON cd.mrn = rp.mrn 