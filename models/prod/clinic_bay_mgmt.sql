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
 -- New Column: Consultation Type for Dashboard
    CASE 
        WHEN cd.consultation_type IN (
            'Pediatric Assessment', 'Online Autism Therapy Assessment', 'Add-On Session', 
            'Online Pediatric Assessment', 'Online Physiotherapy Assessment', 'Physiotherapy Assessment', 
            'Online Speech Therapy Assessment', 'Neurodevelopmental Assessment', 'Educational Asst. - WCJ - IV',
            'Educational Asst. - WCJ - III', 'Autism Therapy Assessment', 'Functional Assessment', 
            'Remedial Assessment', 'Educational Asst. - WCJ – IV plus Nelson Danny', 'UPPA Assessment', 
            'Online UPPA Assessment', 'UPPA Assessments', 'ADOS Assessment', 'IQ Assessment - WISC - IV', 
            'IQ Assessment - WISC', 'OTPT-Walk-In Clinic', 'IQ Assessment- WISC- V', 'Speech-EIC AWAZ', 
            'OTPT-Walk-In Clinic Assessment'
        ) THEN 'Assessment'

        WHEN cd.consultation_type IN (
            'Occupational Therapy Assessment', 'Online Occupational Therapy Assessment', 
            'Speech Therapy Assessment', 'Autism Therapy Consult', 'Occupational Therapy Consult', 
            'Physiotherapy Consult', 'Online Autism Therapy Consult', 'Online Occupational Therapy Consult', 
            'Online Physiotherapy Consult', 'Online Speech Therapy Consult', 'Pediatric Discussion', 
            'Online Pediatric Discussion', 'Online Pediatric/ADHD Discussion - 30 min', 
            'Pediatric/ADHD Discussion - 30 min', 'School visit', 'Joint Session', 'Home Visit', 
            'Online Sp Ed Report Discussion', 'Sp Ed Report Discussion', 'Online Social work services', 
            'Social work services', 'Speech Therapy Consult', 'ECDC Consultant', 'OTPT Walk-In Clinic Follow Up'
        ) THEN 'Consult'

        WHEN cd.consultation_type IN ('Internal Review', 'Phone/Email Query', 'UPPA Fees') 
        THEN 'Dummy'

        WHEN cd.consultation_type IN ('EIC Fees') THEN 'EIC'

        WHEN cd.consultation_type IN ('FRC Connect') THEN 'FRC'

        WHEN cd.consultation_type IN (
            'Star Level 1', 'Star Level 2', 'Online Star Level 2', 'Online Star Level 1', 
            'Fun Club', 'Fun Club for Caregivers', 'Caregiver Support Group', 'Apna Adda', 
            'Online Apna Adda', 'Pre-writing Group', 'Speech Dyad', 'OTPT Dyad', 'AT Dyad', 
            'Feeding Group', 'Online Feeding Group', 'Online Feeding Group', 'Online Feeding Group Discussion', 
            'Online Feeding Group Screening', 'Online Pre Writing Group', 'UDID Camp', 'Social Skills Group', 
            'AT - Social Skills Group', 'Pre-writing Group', 'Prewriting Dyad', 'Social Pragmatic Group', 
            'Online Mealtime Made Easy Group Session', 'Online Mealtime Made Easy Group Discussion', 
            'UBUNTU Add On Session', 'AT - Playdate Group', 'UBUNTU Group Session', 'Letter Writing Dyad'
        ) THEN 'Group'

        WHEN cd.consultation_type IN (
            'Autism Therapy', 'Occupational Therapy', 'Occupational Therapy- 30 min', 'Counselling', 
            'Online Autism Therapy', 'Online Autism Therapy-30 min', 'Online Counselling', 
            'Online Counselling - Caregiver', 'Counselling Caregiver', 'Online Counselling Caregiver 30 mins', 
            'Counselling- 30 min', 'Online Counselling- 30 min', 'Online Occupational Therapy', 
            'Online Occupational Therapy- 30 min', 'Online Physiotherapy', 'Online Physiotherapy- 30 min', 
            'Online Remediation', 'Remediation', 'Online Speech Therapy', 'Online Speech Therapy- 30 min', 
            'UPPA Session', 'Online UPPA Session', 'Speech Therapy', 'Physiotherapy', 'Neurodevelopmental Therapy'
        ) THEN 'Therapy'

        ELSE 'Other'
    END AS consultation_type_dashboard,
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

    -- Calculate Age from Date of Birth
    DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) AS calculated_age,

    -- Assign Age Group
    CASE 
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 0 AND 3 THEN '0-3 yr'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 3.1 AND 6 THEN '3.1-6 yr'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 6.1 AND 9 THEN '6.1-9 yr'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 9.1 AND 14 THEN '9.1-14 yr'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 14.1 AND 19 THEN '14.1-19 yr'
        WHEN DATE_PART('year', AGE(NOW(), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) BETWEEN 19.1 AND 24 THEN '19.1-24 yr'
        ELSE '24.1 and above'
    END AS age_group,

    -- Coalesced Parent/Guardian Details
    rp.parent_guardian_phone,
    rp.parent_guardian_age,
    rp.parent_guardian_city,
    rp.parent_guardian_district,
    rp.parent_guardian_state,
    rp.parent_guardian_country,
    rp.who_brought_the_child,

    -- New column for location categorization
    CASE 
        WHEN LOWER(rp.parent_guardian_city) = 'mumbai' 
             OR LOWER(rp.parent_guardian_district) = 'mumbai' 
             OR LOWER(rp.parent_guardian_state) = 'maharashtra' 
                AND LOWER(rp.parent_guardian_country) = 'india'
        THEN 'Mumbai'
        ELSE 'Non-Mumbai'
    END AS location_category,

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

WHERE 
    cd.consultation_type NOT IN ('Internal Review', 'Phone/ Email Query', 'UPPA Fees')
    AND cd.patient_name <> 'Dummy Ummeed'

