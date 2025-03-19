{{ config(materialized='table') }}

WITH clinic_data AS (
    SELECT 
        mrno::VARCHAR AS mrno,
        patient_name::TEXT AS patient_name,
        patient_age::NUMERIC(38,9) AS patient_age,
        patient_gender::TEXT AS patient_gender,
        mobile_no::VARCHAR AS mobile_no,
        department::TEXT AS department,
        doctor::TEXT AS doctor,
        consultation_date::DATE AS consultation_date,
        consultation_type::TEXT AS consultation_type,
        date_of_birth::TEXT AS date_of_birth,
        diagnosis::TEXT AS diagnosis,
        calculated_age::FLOAT8 AS calculated_age,
        age_group::TEXT AS age_group,
        pat_idn_no::TEXT AS pat_idn_no,
        patient_income::TEXT AS patient_income,
        consultation_category::TEXT AS consultation_category, 
        dep_consult_category::TEXT AS dep_consult_category, 
        dep_shortened::TEXT AS dep_shortened,
        EXTRACT(YEAR FROM consultation_date)::INTEGER AS year,
        -- Calculate Financial Year
        CASE 
            WHEN EXTRACT(MONTH FROM consultation_date) >= 4 
            THEN CONCAT(EXTRACT(YEAR FROM consultation_date), '-', EXTRACT(YEAR FROM consultation_date) + 1)
            ELSE CONCAT(EXTRACT(YEAR FROM consultation_date) - 1, '-', EXTRACT(YEAR FROM consultation_date))
        END::TEXT AS financial_year,
        TO_CHAR(consultation_date, 'Month')::TEXT AS month,
        CAST(NULL AS TEXT) AS course_name,  
        CAST(NULL AS TEXT) AS course_category
    FROM {{ ref('clinic_data') }}
    WHERE 
        TRIM(patient_name) <> 'Dummy Ummeed'
        AND consultation_type NOT IN ('Internal Review', 'Phone/Email Query', 'UPPA Fees')
),

matched_synergy AS (
    SELECT 
        cd.mrno,  -- Only keep rows where MRN exists in clinic_data
        sp.primary_contact::VARCHAR AS mobile_no,
        sp.course_name::TEXT AS course_name,
        sp.course_category::TEXT AS course_category,
        TO_CHAR(cd.consultation_date, 'Month')::TEXT AS month,
        cd.patient_name as patient_name,
        cd.patient_age as patient_age,
        cd.patient_gender as patient_gender,
        cd.date_of_birth as date_of_birth,
        cd.diagnosis as diagnosis,
        cd.calculated_age as calculated_age,
        cd.age_group as age_group,
        cd.pat_idn_no as pat_idn_no,
        cd.patient_income as patient_income,
        sp.start_date as consultation_date,
        EXTRACT(YEAR FROM cd.consultation_date)::INTEGER AS year,
        CASE 
            WHEN EXTRACT(MONTH FROM cd.consultation_date) >= 4 
            THEN CONCAT(EXTRACT(YEAR FROM consultation_date), '-', EXTRACT(YEAR FROM consultation_date) + 1)
            ELSE CONCAT(EXTRACT(YEAR FROM consultation_date) - 1, '-', EXTRACT(YEAR FROM consultation_date))
        END::TEXT AS financial_year
    FROM {{ ref('participant_impact') }} sp
    INNER JOIN {{ ref('clinic_data') }} cd
        ON TRIM(cd.mobile_no) = TRIM(sp.primary_contact)
),

expanded_synergy AS (
    SELECT 
        ms.mrno::VARCHAR AS mrno,
        ms.patient_name AS patient_name,
        ms.patient_age AS patient_age,
        ms.patient_gender AS patient_gender,
        ms.mobile_no::VARCHAR AS mobile_no,
        CAST(NULL AS TEXT) AS department,
        CAST(NULL AS TEXT) AS doctor,
        ms.consultation_date AS consultation_date,
        CAST(NULL AS TEXT) AS consultation_type,
        ms.date_of_birth AS date_of_birth,
        ms.diagnosis AS diagnosis,
        ms.calculated_age AS calculated_age,
        ms.age_group AS age_group,
        ms.pat_idn_no AS pat_idn_no,
        ms.patient_income AS patient_income,
        CAST(NULL AS TEXT) AS consultation_category, 
        CAST(NULL AS TEXT) AS dep_consult_category, 
        CAST(NULL AS TEXT) AS dep_shortened,
        ms.year AS year,
        ms.financial_year,
        ms.month AS month,
        ms.course_name::TEXT AS course_name,
        ms.course_category::TEXT AS course_category
    FROM matched_synergy ms
)

SELECT 
*,
'Clinic Data' AS source
FROM clinic_data

UNION ALL

SELECT 
*, 
'Training' AS source
FROM expanded_synergy