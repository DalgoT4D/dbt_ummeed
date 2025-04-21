{{ config(materialized='table') }}

WITH cleaned_clinic_data AS (
    SELECT 
        mrno,
        patient_name,
        patient_age,
        patient_gender,
        mobile_no,
        department,
        doctor,
        consultation_date,
        consultation_type,
        date_of_birth,
        diagnosis,
        calculated_age,
        age_group,
        pat_idn_no,
        patient_income,
        consultation_category, 
        dep_consult_category, 
        dep_shortened,
        EXTRACT(YEAR FROM consultation_date) AS year,
        -- Calculate Financial Year
        CASE 
            WHEN EXTRACT(MONTH FROM consultation_date) >= 4 
                THEN CONCAT(EXTRACT(YEAR FROM consultation_date), '-', EXTRACT(YEAR FROM consultation_date) + 1)
            ELSE CONCAT(EXTRACT(YEAR FROM consultation_date) - 1, '-', EXTRACT(YEAR FROM consultation_date))
        END::TEXT AS financial_year,
        TO_CHAR(consultation_date, 'Month')::TEXT AS month,
        NULL::TEXT AS course_name,  
        NULL::TEXT AS course_category,
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
),

matched_synergy AS (
    SELECT 
        cd.mrno,  -- Only keep rows where MRN exists in clinic_data
        sp.primary_contact AS mobile_no,
        sp.course_name,
        sp.course_category,
        TO_CHAR(cd.consultation_date, 'Month')::TEXT AS month,
        cd.patient_name,
        cd.department,
        cd.patient_age,
        cd.patient_gender,
        cd.date_of_birth,
        cd.diagnosis,
        cd.calculated_age,
        cd.age_group,
        cd.pat_idn_no,
        cd.patient_income,
        sp.start_date AS consultation_date,
        EXTRACT(YEAR FROM cd.consultation_date) AS year,
        -- Financial Year
        CASE 
            WHEN EXTRACT(MONTH FROM cd.consultation_date) >= 4 
                THEN CONCAT(EXTRACT(YEAR FROM cd.consultation_date), '-', EXTRACT(YEAR FROM cd.consultation_date) + 1)
            ELSE CONCAT(EXTRACT(YEAR FROM cd.consultation_date) - 1, '-', EXTRACT(YEAR FROM cd.consultation_date))
        END::TEXT AS financial_year,
        -- Quarter
        CASE 
            WHEN EXTRACT(MONTH FROM cd.consultation_date) BETWEEN 1 AND 3 
                THEN 'Q4'
            WHEN EXTRACT(MONTH FROM cd.consultation_date) BETWEEN 4 AND 6 
                THEN 'Q1'
            WHEN EXTRACT(MONTH FROM cd.consultation_date) BETWEEN 7 AND 9 
                THEN 'Q2'
            WHEN EXTRACT(MONTH FROM cd.consultation_date) BETWEEN 10 AND 12 
                THEN 'Q3'
        END AS quarter
    FROM {{ ref('participant_impact') }} AS sp
    INNER JOIN {{ ref('clinic_data') }} AS cd
        ON TRIM(cd.mobile_no) = TRIM(sp.primary_contact)
),

expanded_synergy AS (
    SELECT 
        ms.mrno,
        ms.patient_name,
        ms.patient_age,
        ms.patient_gender,
        ms.mobile_no,
        ms.department AS department,
        NULL::TEXT AS doctor,
        ms.consultation_date,
        NULL::TEXT AS consultation_type,
        ms.date_of_birth,
        ms.diagnosis,
        ms.calculated_age,
        ms.age_group,
        ms.pat_idn_no,
        ms.patient_income,
        NULL::TEXT AS consultation_category, 
        NULL::TEXT AS dep_consult_category, 
        NULL::TEXT AS dep_shortened,
        ms.year,
        ms.financial_year,
        ms.month,
        ms.course_name,
        ms.course_category,
        ms.quarter
    FROM matched_synergy AS ms
)

SELECT 
    *,
    'Clinic Data' AS source
FROM cleaned_clinic_data

UNION ALL

SELECT 
    *, 
    'Training' AS source
FROM expanded_synergy
