{{ config(materialized='table') }}

WITH clinic_data AS (
    SELECT
        "encounterid" AS s_no,
        "mrno" AS mrn,
        "patientname" AS patient_name,
        "age" AS patient_age,
        "gender" AS patient_gender,
        "mobileno" AS mobile_no,
        "department" AS department,
        "doctor" AS doctor,
        TO_DATE("consultationrequestdate", 'DD-MON-YY') AS consultation_date,
        "appointmenttype" AS consultation_type,
        "unit" AS unit,
        "visitid" AS visit_no,
        "billstatus" AS billed_status,
        "invoiceno" AS invoice_no,
        "queuegenerationtime" AS queue_generation_time,
        "queueno" AS queue_no,
        "startconsultationtime" AS start_consultation_time,
        "endconsultationtime" AS end_consultation_time,
        "sponsorname" AS sponsor_name,
        "plan" AS plan,
        "assocmpny" AS associate_company
    FROM {{ source('source_ummeed_ict_health', 'clinic_bay_management_data') }}
),

registered_patient AS (
    SELECT 
        id AS registered_patient_id,
        registered_patient_age,
        dob AS date_of_birth,
        mrno AS mrn,
        registered_patient_gender,
        diagnosis,
        mobile_no AS registered_mobile_no,
        pat_idn_no,
        guardian_pin,
        COALESCE(father_phone, mother_phone) AS parent_guardian_phone,
        COALESCE(father_age, guardian_age) AS parent_guardian_age,
        COALESCE(father_city, mother_city, guardian_city) AS parent_guardian_city,
        COALESCE(father_dist, mother_dist, guardian_dist) AS parent_guardian_district,
        COALESCE(father_state, mother_state, guardian_rstate) AS parent_guardian_state,
        COALESCE(father_country, mother_country) AS parent_guardian_country,
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
    cd.*,
    rp.registered_patient_id,
    rp.registered_patient_age,
    rp.date_of_birth,
    rp.registered_patient_gender,
    rp.diagnosis,
    rp.registered_mobile_no,
    DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) AS calculated_age,
    CASE 
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 0 AND 3 THEN '0-3'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 3.1 AND 6 THEN '3.1-6'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 6.1 AND 9 THEN '6.1-9'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 9.1 AND 14 THEN '9.1-14'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 14.1 AND 19 THEN '14.1-19'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 19.1 AND 24 THEN '19.1-24'
        ELSE '24.1 and above'
    END AS age_group,
    rp.parent_guardian_phone,
    rp.parent_guardian_age,
    rp.parent_guardian_city,
    rp.parent_guardian_district,
    rp.parent_guardian_state,
    rp.parent_guardian_country,
    rp.who_brought_the_child,
    CASE 
        WHEN LOWER(rp.parent_guardian_city) = 'mumbai' 
             OR LOWER(rp.parent_guardian_district) = 'mumbai' 
             OR (LOWER(rp.parent_guardian_state) = 'maharashtra' 
                 AND LOWER(rp.parent_guardian_country) = 'india')
        THEN 'Mumbai'
        ELSE 'Non-Mumbai'
    END AS location_category,
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

