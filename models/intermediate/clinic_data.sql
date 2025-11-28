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
        REPLACE(diagnosis, '?', '''') AS diagnosis,
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
),

promotions as (
    select
        doctor_name,
        doctor_lvl,
        promotion_date,
        lead(promotion_date) over (
            partition by doctor_name 
            order by promotion_date
        ) as next_promotion_date
    from {{ source('source_ummeed_ict_health', 'dim_doctor_level_mapping')}}    
),

Base_Clinic_Data AS (
    SELECT 
        cd.*,
        rp.registered_patient_id,
        rp.registered_patient_age,
        rp.date_of_birth,
        rp.registered_patient_gender,
        rp.diagnosis,
        rp.registered_mobile_no::VARCHAR AS registered_mobile_no,
        CASE
            WHEN cd.consultation_date IS NOT NULL THEN
                CASE
                    WHEN EXTRACT(MONTH FROM cd.consultation_date) >= 4 THEN
                        CONCAT('01/04/', EXTRACT(YEAR FROM cd.consultation_date))
                    ELSE
                        CONCAT('01/04/', EXTRACT(YEAR FROM cd.consultation_date) - 1)
                END
            ELSE NULL
        END AS fiscal_year_start_date,    
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
        doctor_lvl  -- Mapped from promotion CTE 
        --COALESCE(p.doctor_level, 'Not Available') AS doctor_level  -- Mapped from promotion CTE 
    FROM clinic_data AS cd
    LEFT JOIN registered_patient AS rp
        ON cd.mrno = rp.mrno
    LEFT JOIN {{ source('source_ummeed_ict_health', 'dim_consultation_type_mapping') }} AS ctm
        ON cd.consultation_type = ctm."Consultation Type"
    LEFT JOIN {{ source('source_ummeed_ict_health', 'dim_department_acronym') }} AS dda
        ON cd.department = dda.department
    LEFT JOIN promotions AS p
        ON cd.doctor = p.doctor_name 
        AND cd.consultation_date >= p.promotion_date
        AND (
          p.next_promotion_date is NULL 
          OR cd.consultation_date < p.next_promotion_date
     )
),

CBD_And_Calculated_Age AS (
SELECT 
    *,
    COALESCE(doctor_lvl, 'Not Available') AS doctor_level,
    CASE
        WHEN bcd.date_of_birth IS NULL OR bcd.fiscal_year_start_date IS NULL THEN NULL
        WHEN TO_DATE(bcd.date_of_birth, 'DD/MM/YYYY')::DATE > TO_DATE(bcd.fiscal_year_start_date, 'DD/MM/YYYY')::DATE THEN CAST(0.00 AS NUMERIC)
        WHEN TO_DATE(bcd.date_of_birth, 'DD/MM/YYYY')::DATE > bcd.consultation_date::DATE THEN CAST(0.00 AS NUMERIC)
        ELSE ROUND(
            (
                (TO_DATE(bcd.fiscal_year_start_date, 'DD/MM/YYYY')::DATE - TO_DATE(bcd.date_of_birth, 'DD/MM/YYYY')::DATE)::NUMERIC
                / CAST(365.25 AS NUMERIC)
            ),
            2
        )::NUMERIC
    END AS calculated_age,
    ROUND(
        (
            (TO_DATE(bcd.consultation_date, 'DD/MM/YYYY')::DATE - TO_DATE(bcd.date_of_birth, 'DD/MM/YYYY')::DATE)::NUMERIC
            / CAST(365.25 AS NUMERIC)
        ),
        2
    )::NUMERIC AS actual_age,
    ROUND(
        (
            (TO_DATE(current_date, 'DD/MM/YYYY')::DATE- TO_DATE(bcd.date_of_birth, 'DD/MM/YYYY')::DATE)::NUMERIC
            / CAST(365.25 AS NUMERIC)
        ),
        2
    )::NUMERIC AS present_age
FROM Base_Clinic_Data AS bcd
),

Complete_Clinic_Data AS (
SELECT
    *,
    CASE
        WHEN calculated_age IS NULL THEN NULL
        WHEN calculated_age >= 0    AND calculated_age <= 3.99  THEN 'Group A: 0 to 3'
        WHEN calculated_age >= 4    AND calculated_age <= 6.99  THEN 'Group B: 4 to 6'
        WHEN calculated_age >= 7    AND calculated_age <= 9.99  THEN 'Group C: 7 to 9'
        WHEN calculated_age >= 10    AND calculated_age <= 12.99 THEN 'Group D: 10 to 12'
        WHEN calculated_age >= 13   AND calculated_age <= 15.99 THEN 'Group E: 13 to 15'
        ELSE 'Group F: 16 and Above'
    END AS age_group
FROM CBD_And_Calculated_Age AS cca)

SELECT
    *
FROM Complete_Clinic_Data
