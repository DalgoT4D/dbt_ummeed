{{ config(materialized='table') }}

SELECT 
    "S.No." AS s_no,
    "MRN" AS mrn,
    "Patient Name" AS patient_name,
    "Age" AS age,
    "Gender" AS gender,
    "Mobile No" AS mobile_no,
    "Department" AS department,
    "Doctor" AS doctor,
    TO_TIMESTAMP("Consultation Request Date/Time", 'DD-MM-YYYY HH24:MI')::DATE AS consultation_date,
    "Consultation Type" AS consultation_type,
    "Unit" AS unit,
    "Visit  No." AS visit_no,
    "Billed Status" AS billed_status,
    "Invoice No" AS invoice_no,
    "Queue Generation Time" AS queue_generation_time,
    "Queue No." AS queue_no,
    "Start Consultation Time" AS start_consultation_time,
    "End Consultation Time" AS end_consultation_time,
    "Sponsor Name" AS sponsor_name,
    "Plan" AS plan,
    "Associate Company" AS associate_company
FROM 
{{ source('source_ummeed_synergy_connect', 'clinic_bay_management_data') }}