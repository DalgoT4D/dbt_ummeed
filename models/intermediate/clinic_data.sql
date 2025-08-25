{{ config(materialized='table') }}

WITH clinic_data AS (
    SELECT
        encounterid::VARCHAR AS s_no,
        mrno::VARCHAR AS mrno,
        patientname AS patient_name,
        CAST(age AS VARCHAR) AS patient_age,
        gender AS patient_gender,
        mobileno::VARCHAR AS mobile_no,
        department::VARCHAR AS department,
        --doctor::VARCHAR AS doctor,
        -- Cleaned doctor name value by removing titles, salutations and extra spaces
        -- The regex removes common titles like Dr., Miss, Ms., Mr., Mrs., and Mister
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                TRIM(
                    REGEXP_REPLACE(doctor, '(?i)(^|\s)(dr\.?|miss|ms\.?|mr\.?|mister|mrs\.?)(\s|$)', ' ')
                ),
                '\s+', ' '
            ),
            '^\s+|\s+$', ''
        )::VARCHAR AS doctor,
        TO_DATE(consultationrequestdate, 'DD-MON-YY') AS consultation_date,
        REPLACE(appointmenttype,'?', '-') AS consultation_type,
        unit,
        visitid AS visit_no,
        billstatus AS billed_status,
        invoiceno AS invoice_no,
        queuegenerationtime AS queue_generation_time,
        queueno AS queue_no,
        startconsultationtime AS start_consultation_time,
        endconsultationtime AS end_consultation_time,
        sponsorname AS sponsor_name,
        plan,
        assocmpny AS associate_company
    FROM {{ source('source_ummeed_ict_health', 'clinic_bay_management_data') }}
),

registered_patient AS (
    SELECT 
        id::VARCHAR AS registered_patient_id,
        registered_patient_age::VARCHAR AS registered_patient_age,
        dob AS date_of_birth,
        mrno,
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
        END AS who_brought_the_child,
        plan_name,
        is_processed::TEXT,
        updated_date,
        inserted_date::DATE AS inserted_date,
        identity_type,
        patient_income,
        registration_type,
        service_center_name
    FROM {{ ref('registered_patient') }}
)

SELECT 
    cd.*,
    rp.registered_patient_id,
    rp.registered_patient_age,
    rp.date_of_birth,
    rp.registered_patient_gender,
    rp.diagnosis,
    rp.registered_mobile_no::VARCHAR AS registered_mobile_no,
    CASE
        WHEN rp.date_of_birth IS NOT NULL AND cd.consultation_date IS NOT NULL THEN
            CASE
                WHEN TO_DATE(rp.date_of_birth, 'DD/MM/YYYY') < cd.consultation_date THEN
                    -- Use consultation_date
                    CASE
                        WHEN EXTRACT(MONTH FROM cd.consultation_date) >= 4 THEN
                            CONCAT('31/03/', EXTRACT(YEAR FROM cd.consultation_date) + 1)
                        ELSE
                            CONCAT('31/03/', EXTRACT(YEAR FROM cd.consultation_date))
                    END
                WHEN EXTRACT(YEAR FROM TO_DATE(rp.date_of_birth, 'DD/MM/YYYY')) = EXTRACT(YEAR FROM cd.consultation_date) THEN
                    -- Same year: use max month
                    CASE
                        WHEN GREATEST(EXTRACT(MONTH FROM TO_DATE(rp.date_of_birth, 'DD/MM/YYYY')), EXTRACT(MONTH FROM cd.consultation_date)) > 4 THEN
                            CONCAT('31/03/', EXTRACT(YEAR FROM TO_DATE(rp.date_of_birth, 'DD/MM/YYYY')) + 1)
                        ELSE
                            CONCAT('31/03/', EXTRACT(YEAR FROM TO_DATE(rp.date_of_birth, 'DD/MM/YYYY')))
                    END
                ELSE
                    -- Different years: use later date
                    CASE
                        WHEN EXTRACT(MONTH FROM 
                            CASE 
                                WHEN EXTRACT(YEAR FROM TO_DATE(rp.date_of_birth, 'DD/MM/YYYY')) > EXTRACT(YEAR FROM cd.consultation_date) 
                                THEN TO_DATE(rp.date_of_birth, 'DD/MM/YYYY') 
                                ELSE cd.consultation_date 
                            END
                        ) > 4 THEN
                            CONCAT('31/03/', GREATEST(EXTRACT(YEAR FROM TO_DATE(rp.date_of_birth, 'DD/MM/YYYY')), EXTRACT(YEAR FROM cd.consultation_date)) + 1)
                        ELSE
                            CONCAT('31/03/', GREATEST(EXTRACT(YEAR FROM TO_DATE(rp.date_of_birth, 'DD/MM/YYYY')), EXTRACT(YEAR FROM cd.consultation_date)))
                    END
            END
        ELSE NULL
    END::DATE AS fiscal_year_end_date,    
    DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY')))::TEXT AS calculated_age,
    CASE
        WHEN rp.date_of_birth IS NULL OR fiscal_year_end_date IS NULL THEN NULL
        ELSE
            CASE
                WHEN DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) < 2 
                THEN 'Group A: 0 ≤ age < 2'
                WHEN DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) >= 2 AND DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) < 4 THEN 'Group B: 2 ≤ age < 4'
                WHEN DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) >= 4 AND DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) < 6 THEN 'Group C: 4 ≤ age < 6'
                WHEN DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) >= 6 AND DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) < 10 THEN 'Group D: 6 ≤ age < 10'
                WHEN DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) >= 10 AND DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) < 12 THEN 'Group E: 10 ≤ age < 12'
                WHEN DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) >= 12 AND DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) < 14 THEN 'Group F: 12 ≤ age < 14'
                WHEN DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) >= 14 AND DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) < 16 THEN 'Group G: 14 ≤ age < 16'
                WHEN DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) >= 16 AND DATE_PART('year', AGE(TO_DATE(fiscal_year_end_date), TO_DATE(rp.date_of_birth, 'DD/MM/YYYY'))) < 18 THEN 'Group H: 16 ≤ age < 18'
                ELSE 'Group I: 18 and above'
            END
    END AS age_group,
    rp.parent_guardian_phone,
    rp.parent_guardian_age,
    rp.parent_guardian_city,
    rp.parent_guardian_district,
    rp.parent_guardian_state,
    rp.parent_guardian_country,
    rp.who_brought_the_child,
    CASE 
        WHEN
            LOWER(rp.parent_guardian_city) = 'mumbai' 
            OR LOWER(rp.parent_guardian_district) = 'mumbai' 
            OR (
                LOWER(rp.parent_guardian_state) = 'maharashtra' 
                AND LOWER(rp.parent_guardian_country) = 'india'
            )
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
    ctm."New Classification" AS consultation_category,  -- Mapped from dim_consultation_type_mapping
    CONCAT_WS(' ', dda.acronym, ctm."New Classification") AS dep_consult_category,  -- Acronym + Consultation Category
    dda.acronym AS dep_shortened,
    ddlm.doctor_level AS doctor_level  -- Mapped from dim_doctor_level_mapping

FROM clinic_data AS cd
LEFT JOIN registered_patient AS rp
    ON cd.mrno = rp.mrno
LEFT JOIN {{ source('source_ummeed_ict_health', 'dim_consultation_type_mapping') }} AS ctm
    ON cd.consultation_type = ctm."Consultation Type"
LEFT JOIN {{ source('source_ummeed_ict_health', 'dim_department_acronym') }} AS dda
    ON cd.department = dda.department
LEFT JOIN {{ source('source_ummeed_ict_health', 'dim_doctor_level_mapping') }} AS ddlm
    ON cd.doctor = ddlm.doctor