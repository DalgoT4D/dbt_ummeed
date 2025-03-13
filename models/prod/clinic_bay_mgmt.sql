{{ config(materialized='table') }}

SELECT
*,
-- Calculate Financial Year
CASE 
    WHEN EXTRACT(MONTH FROM consultation_date) >= 4 
    THEN CONCAT(EXTRACT(YEAR FROM consultation_date), '-', 
                EXTRACT(YEAR FROM consultation_date) + 1)
    ELSE CONCAT(EXTRACT(YEAR FROM consultation_date) - 1, '-', 
                EXTRACT(YEAR FROM consultation_date))
END AS financial_year,
EXTRACT(MONTH FROM consultation_date) AS month

FROM {{ref('clinic_data')}}
WHERE 
    patient_name <> 'Dummy Ummeed'
    AND consultation_type NOT IN ('Internal Review', 'Phone/Email Query', 'UPPA Fees')
