{{ config(materialized='table') }}

SELECT 
*,
-- Calculate Financial Year
CASE 
    WHEN EXTRACT(MONTH FROM event_valid_to) >= 4 
    THEN CONCAT(EXTRACT(YEAR FROM event_valid_to), '-', EXTRACT(YEAR FROM event_valid_to) + 1)
    ELSE CONCAT(EXTRACT(YEAR FROM event_valid_to) - 1, '-', EXTRACT(YEAR FROM event_valid_to))
END AS financial_year,
EXTRACT(MONTH FROM event_valid_to) AS month
FROM {{ref('appointment_data')}}